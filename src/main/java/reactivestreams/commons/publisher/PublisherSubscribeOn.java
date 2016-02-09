package reactivestreams.commons.publisher;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.flow.Loopback;
import reactivestreams.commons.flow.Producer;
import reactivestreams.commons.util.BackpressureHelper;
import reactivestreams.commons.util.DeferredSubscription;
import reactivestreams.commons.util.EmptySubscription;
import reactivestreams.commons.util.ExceptionHelper;
import reactivestreams.commons.util.SubscriptionHelper;

/**
 * Subscribes to the source Publisher asynchronously through a scheduler function or
 * ExecutorService.
 * 
 * @param <T> the value type
 */
public final class PublisherSubscribeOn<T> extends PublisherSource<T, T> implements Loopback {

    final Callable<? extends Consumer<Runnable>> schedulerFactory;
    
    public PublisherSubscribeOn(
            Publisher<? extends T> source, 
            Callable<? extends Consumer<Runnable>> schedulerFactory) {
        super(source);
        this.schedulerFactory = Objects.requireNonNull(schedulerFactory, "schedulerFactory");
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        Consumer<Runnable> scheduler;
        
        try {
            scheduler = schedulerFactory.call();
        } catch (Throwable e) {
            ExceptionHelper.throwIfFatal(e);
            EmptySubscription.error(s, e);
            return;
        }
        
        if (scheduler == null) {
            EmptySubscription.error(s, new NullPointerException("The schedulerFactory returned a null Function"));
            return;
        }
        
        if (source instanceof Supplier) {
            
            @SuppressWarnings("unchecked")
            Supplier<T> supplier = (Supplier<T>) source;
            
            T v = supplier.get();
            
            if (v == null) {
                ScheduledEmptySubscriptionEager parent = new ScheduledEmptySubscriptionEager(s, scheduler);
                s.onSubscribe(parent);
                scheduler.accept(parent);
            } else {
                s.onSubscribe(new ScheduledSubscriptionEagerCancel<>(s, v, scheduler));
            }
            return;
        }
        
        PublisherSubscribeOnClassic<T> parent = new PublisherSubscribeOnClassic<>(s, scheduler);
        //PublisherSubscribeOnPipeline<T> parent = new PublisherSubscribeOnPipeline<>(s, scheduler);
        s.onSubscribe(parent);
        
        scheduler.accept(new SourceSubscribeTask<>(parent, source));
    }

    @Override
    public Object connectedInput() {
        return schedulerFactory;
    }

    @Override
    public Object connectedOutput() {
        return null;
    }

    static final class PublisherSubscribeOnClassic<T>
            extends DeferredSubscription implements Subscriber<T>, Producer, Loopback {
        final Subscriber<? super T> actual;

        final Consumer<Runnable> scheduler;

        public PublisherSubscribeOnClassic(Subscriber<? super T> actual, Consumer<Runnable> scheduler) {
            this.actual = actual;
            this.scheduler = scheduler;
        }

        @Override
        public void onSubscribe(Subscription s) {
            set(s);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            scheduler.accept(null);
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            scheduler.accept(null);
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                scheduler.accept(new RequestTask(n, this));
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            scheduler.accept(null);
        }

        void requestInner(long n) {
            super.request(n);
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object connectedOutput() {
            return scheduler;
        }

        @Override
        public Object connectedInput() {
            return null;
        }
    }

    static final class PublisherSubscribeOnPipeline<T>
            extends DeferredSubscription implements Subscriber<T>, Producer, Loopback, Runnable {
        final Subscriber<? super T> actual;

        final Consumer<Runnable> scheduler;

        volatile long requested;

        @SuppressWarnings("rawtypes")
        static final AtomicLongFieldUpdater<PublisherSubscribeOnPipeline> REQUESTED =
            AtomicLongFieldUpdater.newUpdater(PublisherSubscribeOnPipeline.class, "requested");

        volatile int wip;

        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherSubscribeOnPipeline> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherSubscribeOnPipeline.class, "wip");

        public PublisherSubscribeOnPipeline(Subscriber<? super T> actual, Consumer<Runnable> scheduler) {
            this.actual = actual;
            this.scheduler = scheduler;
        }

        @Override
        public void onSubscribe(Subscription s) {
            set(s);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            scheduler.accept(null);
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            scheduler.accept(null);
            actual.onComplete();
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                BackpressureHelper.addAndGet(REQUESTED, this, n);
                if(WIP.getAndIncrement(this) == 0){
                    scheduler.accept(this);
                }
            }
        }

        @Override
        public void run() {
            long r;
            int missed = 1;
            for(;;){
                r = REQUESTED.getAndSet(this, 0L);

                if(r != 0L) {
                    super.request(r);
                }

                if(r == Long.MAX_VALUE){
                    return;
                }

                missed = WIP.addAndGet(this, -missed);
                if(missed == 0){
                    break;
                }
            }
        }

        @Override
        public void cancel() {
            super.cancel();
            scheduler.accept(null);
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object connectedOutput() {
            return scheduler;
        }

        @Override
        public Object connectedInput() {
            return null;
        }
    }

    static final class RequestTask implements Runnable {

        final long n;
        final PublisherSubscribeOnClassic<?> parent;

        public RequestTask(long n, PublisherSubscribeOnClassic<?> parent) {
            this.n = n;
            this.parent = parent;
        }

        @Override
        public void run() {
            parent.requestInner(n);
        }
    }
    
    static final class ScheduledSubscriptionEagerCancel<T>
            implements Subscription, Runnable, Producer, Loopback {

        final Subscriber<? super T> actual;
        
        final T value;
        
        final Consumer<Runnable> scheduler;

        volatile int once;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<ScheduledSubscriptionEagerCancel> ONCE =
                AtomicIntegerFieldUpdater.newUpdater(ScheduledSubscriptionEagerCancel.class, "once");

        public ScheduledSubscriptionEagerCancel(Subscriber<? super T> actual, T value, Consumer<Runnable> scheduler) {
            this.actual = actual;
            this.value = value;
            this.scheduler = scheduler;
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (ONCE.compareAndSet(this, 0, 1)) {
                    scheduler.accept(this);
                }
            }
        }
        
        @Override
        public void cancel() {
            ONCE.lazySet(this, 1);
            scheduler.accept(null);
        }
        
        @Override
        public void run() {
            actual.onNext(value);
            scheduler.accept(null);
            actual.onComplete();
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public Object connectedInput() {
            return scheduler;
        }

        @Override
        public Object connectedOutput() {
            return value;
        }
    }

    static final class ScheduledEmptySubscriptionEager implements Subscription, Runnable, Producer, Loopback {
        final Subscriber<?> actual;

        final Consumer<Runnable> scheduler;

        public ScheduledEmptySubscriptionEager(Subscriber<?> actual, Consumer<Runnable> scheduler) {
            this.actual = actual;
            this.scheduler = scheduler;
        }

        @Override
        public void request(long n) {
            SubscriptionHelper.validate(n);
        }

        @Override
        public void cancel() {
            scheduler.accept(null);
        }

        @Override
        public void run() {
            scheduler.accept(null);
            actual.onComplete();
        }

        @Override
        public Object connectedInput() {
            return scheduler;
        }

        @Override
        public Object connectedOutput() {
            return null;
        }

        @Override
        public Object downstream() {
            return actual;
        }
    }

    static final class SourceSubscribeTask<T> implements Runnable {

        final Subscriber<? super T> actual;
        
        final Publisher<? extends T> source;

        public SourceSubscribeTask(Subscriber<? super T> s, Publisher<? extends T> source) {
            this.actual = s;
            this.source = source;
        }

        @Override
        public void run() {
            source.subscribe(actual);
        }
    }

}
