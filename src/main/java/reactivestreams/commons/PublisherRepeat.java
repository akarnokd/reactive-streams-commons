package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.MultiSubscriptionArbiter;
import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Repeatedly subscribes to the source and relays its values either
 * indefinitely or a fixed number of times.
 * <p>
 * The times == Long.MAX_VALUE is treated as infinite repeat.
 *
 * @param <T> the value type
 */
public final class PublisherRepeat<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final long times;

    public PublisherRepeat(Publisher<? extends T> source) {
        this(source, Long.MAX_VALUE);
    }

    public PublisherRepeat(Publisher<? extends T> source, long times) {
        if (times < 0L) {
            throw new IllegalArgumentException("times >= 0 required");
        }
        this.source = Objects.requireNonNull(source, "source");
        this.times = times;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        if (times == 0) {
            EmptySubscription.complete(s);
            return;
        }
        
        PublisherRepeatSubscriber<T> parent = new PublisherRepeatSubscriber<>(source, s, times);

        s.onSubscribe(parent.arbiter);
        
        if (!parent.arbiter.isCancelled()) {
            parent.onComplete();
        }
    }
    
    static final class PublisherRepeatSubscriber<T> 
    implements Subscriber<T> {

        final Subscriber<? super T> actual;

        final Publisher<? extends T> source;
        
        final MultiSubscriptionArbiter arbiter;
        
        long remaining;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherRepeatSubscriber> WIP =
                AtomicIntegerFieldUpdater.newUpdater(PublisherRepeatSubscriber.class, "wip");

        public PublisherRepeatSubscriber(Publisher<? extends T> source, Subscriber<? super T> actual, long remaining) {
            this.source = source;
            this.actual = actual;
            this.remaining = remaining;
            this.arbiter = new MultiSubscriptionArbiter();
        }

        @Override
        public void onSubscribe(Subscription s) {
            arbiter.set(s);
        }

        @Override
        public void onNext(T t) {
            actual.onNext(t);
            
            arbiter.producedOne();
        }

        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            long r = remaining;
            if (r != Long.MAX_VALUE) {
                if (r == 0) {
                    actual.onComplete();
                    return;
                }
                remaining = r - 1;
            }
            
            resubscribe();
        }
        
        void resubscribe() {
            if (WIP.getAndIncrement(this) == 0) {
                do {
                    if (arbiter.isCancelled()) {
                        return;
                    }
                    source.subscribe(this);
                    
                } while (WIP.decrementAndGet(this) != 0);
            }
        }
    }
}
