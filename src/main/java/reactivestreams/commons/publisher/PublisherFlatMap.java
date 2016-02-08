/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivestreams.commons.publisher;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.Function;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.flow.Fuseable;
import reactivestreams.commons.flow.MultiReceiver;
import reactivestreams.commons.flow.Producer;
import reactivestreams.commons.flow.Receiver;
import reactivestreams.commons.state.Backpressurable;
import reactivestreams.commons.state.Cancellable;
import reactivestreams.commons.state.Completable;
import reactivestreams.commons.state.Failurable;
import reactivestreams.commons.state.Introspectable;
import reactivestreams.commons.state.Prefetchable;
import reactivestreams.commons.state.Requestable;
import reactivestreams.commons.util.BackpressureHelper;
import reactivestreams.commons.util.CancelledSubscription;
import reactivestreams.commons.util.ExceptionHelper;
import reactivestreams.commons.util.ScalarSubscription;
import reactivestreams.commons.util.SubscriptionHelper;
import reactivestreams.commons.util.UnsignalledExceptions;

/**
 * Maps a sequence of values each into a Publisher and flattens them 
 * back into a single sequence, interleaving events from the various inner Publishers.
 *
 * @param <T> the source value type
 * @param <R> the result value type
 */
public final class PublisherFlatMap<T, R> extends PublisherSource<T, R> {

    final Function<? super T, ? extends Publisher<? extends R>> mapper;
    
    final boolean delayError;
    
    final int maxConcurrency;
    
    final Supplier<? extends Queue<R>> mainQueueSupplier;

    final int prefetch;
    
    final Supplier<? extends Queue<R>> innerQueueSupplier;
    
    public PublisherFlatMap(Publisher<? extends T> source, Function<? super T, ? extends Publisher<? extends R>> mapper,
            boolean delayError, int maxConcurrency, Supplier<? extends Queue<R>> mainQueueSupplier, int prefetch, Supplier<? extends Queue<R>> innerQueueSupplier) {
        super(source);
        if (prefetch <= 0) {
            throw new IllegalArgumentException("prefetch > 0 required but it was " + prefetch);
        }
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency > 0 required but it was " + maxConcurrency);
        }
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        this.delayError = delayError;
        this.prefetch = prefetch;
        this.maxConcurrency = maxConcurrency;
        this.mainQueueSupplier = Objects.requireNonNull(mainQueueSupplier, "mainQueueSupplier");
        this.innerQueueSupplier = Objects.requireNonNull(innerQueueSupplier, "innerQueueSupplier");
    }

    @Override
    public void subscribe(Subscriber<? super R> s) {
        
        if (ScalarSubscription.trySubscribeScalarMap(source, s, mapper)) {
            return;
        }
        
        source.subscribe(new PublisherFlatMapMain<>(s, mapper, delayError, maxConcurrency, mainQueueSupplier, prefetch, innerQueueSupplier));
    }

    static final class PublisherFlatMapMain<T, R> 
    implements Subscriber<T>, Subscription, Receiver, MultiReceiver, Requestable, Completable, Producer,
               Cancellable, Backpressurable, Failurable {
        
        final Subscriber<? super R> actual;

        final Function<? super T, ? extends Publisher<? extends R>> mapper;
        
        final boolean delayError;
        
        final int maxConcurrency;
        
        final Supplier<? extends Queue<R>> mainQueueSupplier;

        final int prefetch;

        final Supplier<? extends Queue<R>> innerQueueSupplier;
        
        final int limit;
        
        volatile Queue<R> scalarQueue;
        
        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherFlatMapMain, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherFlatMapMain.class, Throwable.class, "error");
        
        volatile boolean done;
        
        volatile boolean cancelled;
        
        Subscription s;
        
        volatile long requested;
        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherFlatMapMain> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(PublisherFlatMapMain.class, "requested");
        
        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherFlatMapMain> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherFlatMapMain.class, "wip");

        final int mask;
        
        long consumerIndex;
        ARA<PublisherFlatMapInner<R>> consumerArray;
        
        long producerIndex;
        ARA<PublisherFlatMapInner<R>> producerArray;
        
        static final Object TOMBSTONE = new Object();
        
        static final Object NEXT = new Object();
        
        int produced;
        
        public PublisherFlatMapMain(Subscriber<? super R> actual,
                Function<? super T, ? extends Publisher<? extends R>> mapper, boolean delayError, int maxConcurrency,
                Supplier<? extends Queue<R>> mainQueueSupplier, int prefetch, Supplier<? extends Queue<R>> innerQueueSupplier) {
            this.actual = actual;
            this.mapper = mapper;
            this.delayError = delayError;
            this.maxConcurrency = maxConcurrency;
            this.mainQueueSupplier = mainQueueSupplier;
            this.prefetch = prefetch;
            this.innerQueueSupplier = innerQueueSupplier;
            this.limit = maxConcurrency - (maxConcurrency >> 2);
            this.mask = 15;
            this.consumerArray = this.producerArray = new ARA<>(16);
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.addAndGet(REQUESTED, this, n);
                drain();
            }
        }

        @Override
        public void cancel() {
            if (!cancelled) {
                cancelled = true;
                
                s.cancel();
                
                if (WIP.getAndIncrement(this) == 0) {
                    scalarQueue = null;
                    cancelAllInner();
                }
            }
        }

        @SuppressWarnings("unchecked")
        void cancelAllInner() {
            ARA<PublisherFlatMapInner<R>> a = consumerArray;
            int m = mask + 1;
            while (a != null) {
                for (int i = 0; i < m; i++) {
                    Object o = a.lvElement(i);
                    if (o == null) {
                        break;
                    }
                    if (o != TOMBSTONE && o != NEXT) {
                        ((PublisherFlatMapInner<R>)o).cancel();
                    }
                }
                a = a.lvNext();
            }
        }
        
        boolean add(PublisherFlatMapInner<R> inner, long index) {
            ARA<PublisherFlatMapInner<R>> a = producerArray;
            int m = mask;
            
            int offset = (int)(index + 1) & mask;
            
            Object o = a.get(offset);
            if (o != null) {
                offset = (int)index & mask;
                ARA<PublisherFlatMapInner<R>> b = new ARA<>(m + 2);
                b.soElement(offset, inner);
                a.soNext(b);
                producerArray = b;
                producerIndex = index + 1;
                a.soElementSpecial(offset, NEXT);
            } else {
                offset = (int)index & mask;
                producerIndex = index + 1;
                a.soElement(offset, inner);
            }
            return cancelled;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
                
                if (maxConcurrency == Integer.MAX_VALUE) {
                    s.request(Long.MAX_VALUE);
                } else {
                    s.request(maxConcurrency);
                }
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }
            
            Publisher<? extends R> p;
            
            try {
                p = mapper.apply(t);
            } catch (Throwable e) {
                s.cancel();
                ExceptionHelper.throwIfFatal(e);
                onError(e);
                return;
            }
            
            if (p == null) {
                s.cancel();

                onError(new NullPointerException("The mapper returned a null Publisher"));
                return;
            }
            
            if (p instanceof Supplier) {
                R v;
                try {
                    v = ((Supplier<R>)p).get();
                } catch (Throwable e) {
                    s.cancel();
                    onError(ExceptionHelper.unwrap(e));
                    return;
                }
                emitScalar(v);
            } else {
                long id = producerIndex;
                PublisherFlatMapInner<R> inner = new PublisherFlatMapInner<>(this, prefetch, id);
                if (add(inner, id)) {
                    
                    p.subscribe(inner);
                }
            }
            
        }
        
        void emitScalar(R v) {
            if (v == null) {
                if (maxConcurrency != Integer.MAX_VALUE) {
                    int p = produced + 1;
                    if (p == limit) {
                        produced = 0;
                        s.request(p);
                    } else {
                        produced = p;
                    }
                }
                return;
            }
            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                long r = requested;
                
                if (r != 0L) {
                    actual.onNext(v);
                    
                    if (r != Long.MAX_VALUE) {
                        REQUESTED.decrementAndGet(this);
                    }
                    
                    if (maxConcurrency != Integer.MAX_VALUE) {
                        int p = produced + 1;
                        if (p == limit) {
                            produced = 0;
                            s.request(p);
                        } else {
                            produced = p;
                        }
                    }
                } else {
                    Queue<R> q;
                    
                    try {
                        q = getOrCreateScalarQueue();
                    } catch (Throwable ex) {
                        ExceptionHelper.throwIfFatal(ex);
                        
                        s.cancel();
                        
                        if (ExceptionHelper.addThrowable(ERROR, this, ex)) {
                            done = true;
                        } else {
                            UnsignalledExceptions.onErrorDropped(ex);
                        }
                        
                        drainLoop();
                        return;
                    }
                    
                    if (!q.offer(v)) {
                        s.cancel();
                        
                        Throwable e = new IllegalStateException("Scalar queue full?!");
                        
                        if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                            done = true;
                        } else {
                            UnsignalledExceptions.onErrorDropped(e);
                        }
                        drainLoop();
                        return;
                    }
                }
                if (WIP.decrementAndGet(this) == 0) {
                    return;
                }

                drainLoop();
            } else {
                Queue<R> q;
                
                try {
                    q = getOrCreateScalarQueue();
                } catch (Throwable ex) {
                    ExceptionHelper.throwIfFatal(ex);
                    
                    s.cancel();
                    
                    if (ExceptionHelper.addThrowable(ERROR, this, ex)) {
                        done = true;
                    } else {
                        UnsignalledExceptions.onErrorDropped(ex);
                    }
                    
                    drain();
                    return;
                }
                
                if (!q.offer(v)) {
                    s.cancel();
                    
                    Throwable e = new IllegalStateException("Scalar queue full?!");
                    
                    if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                        done = true;
                    } else {
                        UnsignalledExceptions.onErrorDropped(e);
                    }
                }
                drain();
            }
        }
        
        Queue<R> getOrCreateScalarQueue() {
            Queue<R> q = scalarQueue;
            if (q == null) {
                q = mainQueueSupplier.get();
                scalarQueue = q;
            }
            return q;
        }

        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            if (ExceptionHelper.addThrowable(ERROR, this, t)) {
                done = true;
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }

        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            
            done = true;
            drain();
        }
        
        void drain() {
            if (WIP.getAndIncrement(this) != 0) {
                return;
            }
            drainLoop();
        }
        
        void drainLoop() {
            int missed = 1;
            
            final Subscriber<? super R> a = actual;
            
            for (;;) {
                
                boolean d = done;
                Queue<R> sq = scalarQueue;

                ARA<PublisherFlatMapInner<R>> array = consumerArray;
                int m = mask;
                long ci = consumerIndex;
                
                Object o = array.lvElement((int)ci & m);
                boolean noSources = o == null || o == TOMBSTONE;
                
                if (checkTerminated(d, noSources && (sq == null || sq.isEmpty()), a)) {
                    return;
                }
                
                boolean again = false;

                long r = requested;
                long e = 0L;
                long replenishMain = 0L;
                
                if (r != 0L) {
                    sq = scalarQueue;
                    if (sq != null) {
                        
                        while (e != r) {
                            d = done;
                            
                            R v = sq.poll();
                            
                            boolean empty = v == null;

                            if (checkTerminated(d, false, a)) {
                                return;
                            }
                            
                            if (empty) {
                                break;
                            }
                            
                            a.onNext(v);
                            
                            e++;
                        }
                        
                        if (e != 0L) {
                            replenishMain += e;
                            if (r != Long.MAX_VALUE) {
                                r = REQUESTED.addAndGet(this, -e);
                            }
                            e = 0L;
                            again = true;
                        }
                        
                    }
                }
                if (r != 0L && !noSources) {
                    
                    for (;;) {
                        if (cancelled) {
                            scalarQueue = null;
                            s.cancel();
                            cancelAllInner();
                            return;
                        }
                        
                        int offset = (int)ci & m;
                        
                        o = array.get(offset);
                        
                        if (o == NEXT) {
                            array = array.lvNext();
                            continue;
                        }
                        
                        if (o == null) {
                            break;
                        }
                        
                        if (o != TOMBSTONE) {
                            @SuppressWarnings("unchecked")
                            PublisherFlatMapInner<R> inner = (PublisherFlatMapInner<R>)o;

                            d = inner.done;
                            Queue<R> q = inner.queue;
                            if (d && q == null) {
                                array.soElementSpecial(offset, TOMBSTONE);
                                array.remaining--;
                                again = true;
                                replenishMain++;
                            } else 
                            if (q != null) {
                                while (e != r) {
                                    d = inner.done;
                                    
                                    R v;
                                    
                                    try {
                                        v = q.poll();
                                    } catch (Throwable ex) {
                                        ExceptionHelper.throwIfFatal(ex);
                                        inner.cancel();
                                        if (!ExceptionHelper.addThrowable(ERROR, this, ex)) {
                                            UnsignalledExceptions.onErrorDropped(ex);
                                        }
                                        v = null;
                                        d = true;
                                    }
                                    
                                    boolean empty = v == null;
                                    
                                    if (checkTerminated(d, false, a)) {
                                        return;
                                    }
    
                                    if (d && empty) {
                                        array.soElementSpecial(offset, TOMBSTONE);
                                        array.remaining--;
                                        again = true;
                                        replenishMain++;
                                        break;
                                    }
                                    
                                    if (empty) {
                                        break;
                                    }
                                    
                                    a.onNext(v);
                                    
                                    e++;
                                }
                                
                                if (e == r) {
                                    d = inner.done;
                                    boolean empty;
                                    
                                    try {
                                        empty = q.isEmpty();
                                    } catch (Throwable ex) {
                                        ExceptionHelper.throwIfFatal(ex);
                                        inner.cancel();
                                        if (!ExceptionHelper.addThrowable(ERROR, this, ex)) {
                                            UnsignalledExceptions.onErrorDropped(ex);
                                        }
                                        empty = true;
                                        d = true;
                                    }
                                    
                                    if (d && empty) {
                                        array.soElementSpecial(offset, TOMBSTONE);
                                        array.remaining--;
                                        replenishMain++;
                                    }
                                }
                                
                                if (e != 0L) {
                                    if (!inner.done) {
                                        inner.request(e);
                                    }
                                    if (r != Long.MAX_VALUE) {
                                        r = REQUESTED.addAndGet(this, -e);
                                        if (r == 0L) {
                                            break; // 0 .. n - 1
                                        }
                                    }
                                    e = 0L;
                                }
                            }
                            
                            if (r == 0L) {
                                break;
                            }
                        }
                    }
                }
                
                if (r == 0L && !noSources) {
                    array = consumerArray;
                    long idx = consumerIndex;
                    
                    for (;;) {
                        if (cancelled) {
                            scalarQueue = null;
                            s.cancel();
                            cancelAllInner();
                            return;
                        }
                    
                        int offset = (int)idx & m;
                        
                        o = array.get(offset);
                        
                        if (o == NEXT) {
                            array = array.lvNext();
                            continue;
                        }
                        
                        if (o == null) {
                            break;
                        }
                        
                        if (o != TOMBSTONE) {
                            @SuppressWarnings("unchecked")
                            PublisherFlatMapInner<R> inner = (PublisherFlatMapInner<R>)o;
                            
                            d = inner.done;
                            Queue<R> q = inner.queue;
                            if (d && (q == null || q.isEmpty())) {
                                array.soElementSpecial(offset, TOMBSTONE);
                                array.remaining--;
                                again = true;
                                replenishMain++;
                            }
                        }
                        
                        idx++;
                    }
                }
                
                if (replenishMain != 0L && !done && !cancelled) {
                    s.request(replenishMain);
                }
                
                if (again) {
                    array = consumerArray;
                    ARA<PublisherFlatMapInner<R>> prev = null;
                    
                    while (array != null) {
                        ARA<PublisherFlatMapInner<R>> next = array.lvNext();
                        if (array.remaining == 0) {
                            if (prev == null && next != null) {
                                consumerIndex += m + 1;
                                consumerArray = next;
                                array = next;
                            } else
                            if (prev != null && next != null) {
                                prev.soNext(next);
                            } else {
                                prev = array;
                                array = next;
                            }
                        } else {
                            prev = array;
                            array = next;
                        }
                    }
                    
                    
                    continue;
                }
                
                missed = WIP.addAndGet(this, -missed);
                if (missed == 0) {
                    break;
                }
            }
        }
        
        boolean checkTerminated(boolean d, boolean empty, Subscriber<?> a) {
            if (cancelled) {
                scalarQueue = null;
                s.cancel();
                cancelAllInner();
                
                return true;
            }
            
            if (delayError) {
                if (d && empty) {
                    Throwable e = error;
                    if (e != null && e != ExceptionHelper.TERMINATED) {
                        e = ExceptionHelper.terminate(ERROR, this);
                        a.onError(e);
                    } else {
                        a.onComplete();
                    }
                    
                    return true;
                }
            } else {
                if (d) {
                    Throwable e = error;
                    if (e != null && e != ExceptionHelper.TERMINATED) {
                        e = ExceptionHelper.terminate(ERROR, this);
                        scalarQueue = null;
                        s.cancel();
                        cancelAllInner();
                        
                        a.onError(e);
                        return true;
                    } else 
                    if (empty) {
                        a.onComplete();
                        return true;
                    }
                }
            }
            
            return false;
        }
        
        void innerError(PublisherFlatMapInner<R> inner, Throwable e) {
            if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                inner.done = true;
                if (!delayError) {
                    done = true;
                }
                drain();
            } else {
                UnsignalledExceptions.onErrorDropped(e);
            }
        }
        
        void innerNext(PublisherFlatMapInner<R> inner, R v) {
            if (wip == 0 && WIP.compareAndSet(this, 0, 1)) {
                long r = requested;
                
                if (r != 0L) {
                    actual.onNext(v);
                    
                    if (r != Long.MAX_VALUE) {
                        REQUESTED.decrementAndGet(this);
                    }
                    
                    inner.request(1);
                } else {
                    Queue<R> q;
                    
                    try {
                        q = getOrCreateScalarQueue(inner);
                    } catch (Throwable ex) {
                        ExceptionHelper.throwIfFatal(ex);
                        inner.cancel();
                        if (ExceptionHelper.addThrowable(ERROR, this, ex)) {
                            inner.done = true;
                        } else {
                            UnsignalledExceptions.onErrorDropped(ex);
                        }
                        drainLoop();
                        return;
                    }
                    
                    if (!q.offer(v)) {
                        inner.cancel();
                        
                        Throwable e = new IllegalStateException("Scalar queue full?!");
                        
                        if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                            inner.done = true;
                        } else {
                            UnsignalledExceptions.onErrorDropped(e);
                        }
                        drainLoop();
                        return;
                    }
                }
                if (WIP.decrementAndGet(this) == 0) {
                    return;
                }
                
                drainLoop();
            } else {
                Queue<R> q;
                
                try {
                    q = getOrCreateScalarQueue(inner);
                } catch (Throwable ex) {
                    ExceptionHelper.throwIfFatal(ex);
                    inner.cancel();
                    if (ExceptionHelper.addThrowable(ERROR, this, ex)) {
                        inner.done = true;
                    } else {
                        UnsignalledExceptions.onErrorDropped(ex);
                    }
                    drain();
                    return;
                }
                
                if (!q.offer(v)) {
                    inner.cancel();
                    
                    Throwable e = new IllegalStateException("Scalar queue full?!");
                    
                    if (ExceptionHelper.addThrowable(ERROR, this, e)) {
                        inner.done = true;
                    } else {
                        UnsignalledExceptions.onErrorDropped(e);
                    }
                }
                drain();
            }
        }
        
        Queue<R> getOrCreateScalarQueue(PublisherFlatMapInner<R> inner) {
            Queue<R> q = inner.queue;
            if (q == null) {
                q = innerQueueSupplier.get();
                inner.queue = q;
            }
            return q;
        }

        @Override
        public long getCapacity() {
            return maxConcurrency;
        }

        @Override
        public long getPending() {
            return done || scalarQueue == null ? -1L : scalarQueue.size();
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isStarted() {
            return s != null && !isTerminated() && !isCancelled();
        }

        @Override
        public boolean isTerminated() {
            return done && consumerArray.lvElement((int)consumerIndex & mask) == null;
        }

        @Override
        public Throwable getError() {
            return error;
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public Iterator<?> upstreams() {
            List<Object> list = new ArrayList<>();

            int m = mask + 1;

            ARA<PublisherFlatMapInner<R>> a = consumerArray;
            
            while (a != null) {
                for (int i = 0; i < m; i++) {
                    Object o = a.lvElement(i);
                    if (o != TOMBSTONE && o != NEXT && o != null) {
                        list.add(o);
                    }
                }
            }
            
            return list.iterator();
        }

        @Override
        public long upstreamCount() {
            int count = 0;
            int m = mask + 1;

            ARA<PublisherFlatMapInner<R>> a = consumerArray;
            
            while (a != null) {
                for (int i = 0; i < m; i++) {
                    Object o = a.lvElement(i);
                    if (o != TOMBSTONE && o != NEXT && o != null) {
                        count++;
                    }
                }
            }
            return count;
        }

        @Override
        public long requestedFromDownstream() {
            return requested;
        }

        @Override
        public Object downstream() {
            return actual;
        }
    }
    
    static final class PublisherFlatMapInner<R> 
    implements Subscriber<R>, Subscription, Producer, Receiver,
               Backpressurable,
               Cancellable,
               Completable,
               Prefetchable,
               Introspectable {

        final PublisherFlatMapMain<?, R> parent;
        
        final int prefetch;
        
        final int limit;
        
        final long id;
        
        volatile Subscription s;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherFlatMapInner, Subscription> S =
                AtomicReferenceFieldUpdater.newUpdater(PublisherFlatMapInner.class, Subscription.class, "s");
        
        long produced;
        
        volatile Queue<R> queue;
        
        volatile boolean done;
        
        /** Represents the optimization mode of this inner subscriber. */
        int sourceMode;
        
        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherFlatMapInner> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(PublisherFlatMapInner.class, "once");
        
        public PublisherFlatMapInner(PublisherFlatMapMain<?, R> parent, int prefetch, long index) {
            this.parent = parent;
            this.prefetch = prefetch;
            this.id = index;
            this.limit = prefetch - (prefetch >> 2);
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.setOnce(S, this, s)) {
                if (s instanceof Fuseable.QueueSubscription) {
                    @SuppressWarnings("unchecked") Fuseable.QueueSubscription<R> f = (Fuseable.QueueSubscription<R>)s;
                    int m = f.requestFusion(Fuseable.ANY);
                    if (m == Fuseable.SYNC){
                        sourceMode = Fuseable.SYNC;
                        queue = f;
                        done = true;
                        parent.drain();
                        return;
                    } else 
                    if (m == Fuseable.ASYNC) {
                        sourceMode = Fuseable.ASYNC;
                        queue = f;
                    }
                    // NONE is just fall-through as the queue will be created on demand
                }
                s.request(prefetch);
            }
        }

        @Override
        public void onNext(R t) {
            if (sourceMode == Fuseable.ASYNC) {
                parent.drain();
            } else {
                parent.innerNext(this, t);
            }
        }

        @Override
        public void onError(Throwable t) {
            // we don't want to emit the same error twice in case of subscription-race in async mode
            if (sourceMode != Fuseable.ASYNC || ONCE.compareAndSet(this, 0, 1)) {
                parent.innerError(this, t);
            }
        }

        @Override
        public void onComplete() {
            // onComplete is practically idempotent so there is no risk due to subscription-race in async mode
            done = true;
            parent.drain();
        }

        @Override
        public void request(long n) {
            if (sourceMode != Fuseable.SYNC) {
                long p = produced + n;
                if (p >= limit) {
                    produced = 0L;
                    s.request(p);
                } else {
                    produced = p;
                }
            }
        }

        @Override
        public void cancel() {
            SubscriptionHelper.terminate(S, this);
        }

        @Override
        public long getCapacity() {
            return prefetch;
        }

        @Override
        public long getPending() {
            return done || queue == null ? -1L : queue.size();
        }

        @Override
        public boolean isCancelled() {
            return s == CancelledSubscription.INSTANCE;
        }

        @Override
        public boolean isStarted() {
            return s != null && !done && !isCancelled();
        }

        @Override
        public boolean isTerminated() {
            return done && (queue == null || queue.isEmpty());
        }

        @Override
        public int getMode() {
            return INNER;
        }

        @Override
        public String getName() {
            return PublisherFlatMapInner.class.getSimpleName();
        }

        @Override
        public long expectedFromUpstream() {
            return produced;
        }

        @Override
        public long limit() {
            return limit;
        }

        @Override
        public Object upstream() {
            return s;
        }

        @Override
        public Object downstream() {
            return parent;
        }
    }
    
    static final class ARA<E> extends AtomicReferenceArray<Object> {

        /** */
        private static final long serialVersionUID = 6887678375436293958L;

        final int nextOffset;
        
        int remaining;
        
        public ARA(int length) {
            super(length + 1);
            nextOffset = length;
            remaining = length;
        }

        public void soElement(int offset, E e) {
            lazySet(offset, e);
        }
        
        @SuppressWarnings("unchecked")
        public E lvElement(int offset) {
            return (E)get(offset);
        }
        
        @SuppressWarnings("unchecked")
        public ARA<E> lvNext() {
            return (ARA<E>)get(nextOffset);
        }
        
        public void soNext(ARA<E> next) {
            lazySet(nextOffset, next);
        }
        
        public void soElementSpecial(int offset, Object o) {
            lazySet(offset, o);
        }
    }
}
