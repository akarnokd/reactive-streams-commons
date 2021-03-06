package reactivestreams.commons;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.function.BiFunction;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import reactivestreams.commons.internal.SubscriptionHelper;
import reactivestreams.commons.internal.subscribers.SerializedSubscriber;
import reactivestreams.commons.internal.subscriptions.CancelledSubscription;
import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Combines values from a main Publisher with values from another
 * Publisher through a bi-function and emits the result.
 * 
 * <p>
 * The operator will drop values from the main source until the other
 * Publisher produces any value.
 * <p>
 * If the other Publisher completes without any value, the sequence is completed.
 * 
 * @param <T> the main source type
 * @param <U> the alternate source type
 * @param <R> the output type
 */
public final class PublisherWithLatestFrom<T, U, R> implements Publisher<R> {
    final Publisher<? extends T> source;
    
    final Publisher<? extends U> other;
    
    final BiFunction<? super T, ? super U, ? extends R> combiner;

    public PublisherWithLatestFrom(Publisher<? extends T> source, Publisher<? extends U> other,
            BiFunction<? super T, ? super U, ? extends R> combiner) {
        this.source = Objects.requireNonNull(source, "source");
        this.other = Objects.requireNonNull(other, "other");
        this.combiner = Objects.requireNonNull(combiner, "combiner");
    }
    
    @Override
    public void subscribe(Subscriber<? super R> s) {
        SerializedSubscriber<R> serial = new SerializedSubscriber<>(s);
        
        PublisherWithLatestFromSubscriber<T, U, R> main = new PublisherWithLatestFromSubscriber<>(serial, combiner);
        
        PublisherWithLatestFromOtherSubscriber<U> secondary = new PublisherWithLatestFromOtherSubscriber<>(main);
        
        other.subscribe(secondary);
        
        source.subscribe(main);
    }
    
    static final class PublisherWithLatestFromSubscriber<T, U, R>
    implements Subscriber<T>, Subscription {
        final Subscriber<? super R> actual;
        
        final BiFunction<? super T, ? super U, ? extends R> combiner;

        volatile Subscription main;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherWithLatestFromSubscriber, Subscription> MAIN =
                AtomicReferenceFieldUpdater.newUpdater(PublisherWithLatestFromSubscriber.class, Subscription.class, "main");
        
        volatile Subscription other;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherWithLatestFromSubscriber, Subscription> OTHER =
                AtomicReferenceFieldUpdater.newUpdater(PublisherWithLatestFromSubscriber.class, Subscription.class, "other");

        volatile U otherValue;
        
        public PublisherWithLatestFromSubscriber(Subscriber<? super R> actual,
                BiFunction<? super T, ? super U, ? extends R> combiner) {
            this.actual = actual;
            this.combiner = combiner;
        }

        void setOther(Subscription s) {
            if (!OTHER.compareAndSet(this, null, s)) {
                s.cancel();
                if (other != CancelledSubscription.INSTANCE) {
                    SubscriptionHelper.reportSubscriptionSet();
                }
            }
        }

        @Override
        public void request(long n) {
            main.request(n);
        }

        void cancelMain() {
            Subscription s = main;
            if (s != CancelledSubscription.INSTANCE) {
                s = MAIN.getAndSet(this, CancelledSubscription.INSTANCE);
                if (s != null && s != CancelledSubscription.INSTANCE) {
                    s.cancel();
                }
            }
        }

        void cancelOther() {
            Subscription s = other;
            if (s != CancelledSubscription.INSTANCE) {
                s = OTHER.getAndSet(this, CancelledSubscription.INSTANCE);
                if (s != null && s != CancelledSubscription.INSTANCE) {
                    s.cancel();
                }
            }
        }
        
        @Override
        public void cancel() {
            cancelMain();
            cancelOther();
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (!MAIN.compareAndSet(this, null, s)) {
                s.cancel();
                if (main != CancelledSubscription.INSTANCE) {
                    SubscriptionHelper.reportSubscriptionSet();
                }
            } else {
                actual.onSubscribe(this);
            }
        }

        @Override
        public void onNext(T t) {
            U u = otherValue;
            
            if (u != null) {
                R r;
                
                try {
                    r = combiner.apply(t, u);
                } catch (Throwable e) {
                    onError(e);
                    return;
                }
                
                if (r == null) {
                    onError(new NullPointerException("The combiner returned a null value"));
                    return;
                }
                
                actual.onNext(r);
            } else {
                main.request(1);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (main == null) {
                if (MAIN.compareAndSet(this, null, CancelledSubscription.INSTANCE)) {
                    EmptySubscription.error(actual, t);
                    return;
                }
            }
            cancel();
            
            otherValue = null;
            actual.onError(t);
        }

        @Override
        public void onComplete() {
            cancelOther();
            
            otherValue = null;
            actual.onComplete();
        }

    }
    
    static final class PublisherWithLatestFromOtherSubscriber<U> implements Subscriber<U> {
        final PublisherWithLatestFromSubscriber<?, U, ?> main;

        public PublisherWithLatestFromOtherSubscriber(PublisherWithLatestFromSubscriber<?, U, ?> main) {
            this.main = main;
        }

        @Override
        public void onSubscribe(Subscription s) {
            main.setOther(s);
            
            s.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(U t) {
            main.otherValue = t;
        }

        @Override
        public void onError(Throwable t) {
            main.onError(t);
        }

        @Override
        public void onComplete() {
            PublisherWithLatestFromSubscriber<?, U, ?> m = main;
            if (m.otherValue == null) {
                m.cancelMain();
                
                m.onComplete();
            }
        }
        
        
    }
}
