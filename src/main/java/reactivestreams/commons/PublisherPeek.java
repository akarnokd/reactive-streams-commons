package reactivestreams.commons;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Peek into the lifecycle events and signals of a sequence.
 *
 * <p>
 * The callbacks are all optional.
 * 
 * <p>
 * Crashes by the lambdas are ignored.
 * 
 * @param <T> the value type
 */
public final class PublisherPeek<T> implements Publisher<T> {

    final Publisher<? extends T> source;
    
    final Consumer<? super Subscription> onSubscribeCall;
    
    final Consumer<? super T> onNextCall;
    
    final Consumer<? super Throwable> onErrorCall;
    
    final Runnable onCompleteCall;
    
    final Runnable onAfterTerminateCall;
    
    final LongConsumer onRequestCall;
    
    final Runnable onCancelCall;

    public PublisherPeek(Publisher<? extends T> source, Consumer<? super Subscription> onSubscribeCall,
            Consumer<? super T> onNextCall, Consumer<? super Throwable> onErrorCall, Runnable onCompleteCall,
            Runnable onAfterTerminateCall, LongConsumer onRequestCall, Runnable onCancelCall) {
        this.source = Objects.requireNonNull(source, "source");
        this.onSubscribeCall = onSubscribeCall;
        this.onNextCall = onNextCall;
        this.onErrorCall = onErrorCall;
        this.onCompleteCall = onCompleteCall;
        this.onAfterTerminateCall = onAfterTerminateCall;
        this.onRequestCall = onRequestCall;
        this.onCancelCall = onCancelCall;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherPeekSubscriber<>(s, this));
    }
    
    static final class PublisherPeekSubscriber<T> implements Subscriber<T>, Subscription {
        
        final Subscriber<? super T> actual;
        
        final PublisherPeek<T> parent;
        
        Subscription s;

        public PublisherPeekSubscriber(Subscriber<? super T> actual, PublisherPeek<T> parent) {
            this.actual = actual;
            this.parent = parent;
        }

        @Override
        public void request(long n) {
            try {
                parent.onRequestCall.accept(n);
            } catch (Throwable e) {
                // FIXME nowhere to go
            }
            s.request(n);
        }

        @Override
        public void cancel() {
            try {
                parent.onCancelCall.run();
            } catch (Throwable e) {
             // FIXME nowhere to go
            }
            s.cancel();
        }

        @Override
        public void onSubscribe(Subscription s) {
            try {
                parent.onSubscribeCall.accept(s);
            } catch (Throwable e) {
             // FIXME nowhere to go
            }
            this.s = s;
            actual.onSubscribe(this);
        }

        @Override
        public void onNext(T t) {
            try {
                parent.onNextCall.accept(t);
            } catch (Throwable e) {
             // FIXME nowhere to go
            }
            actual.onNext(t);
        }

        @Override
        public void onError(Throwable t) {
            try {
                parent.onErrorCall.accept(t);
            } catch (Throwable e) {
             // FIXME nowhere to go
            }
            
            actual.onError(t);

            try {
                parent.onAfterTerminateCall.run();
            } catch (Throwable e) {
             // FIXME nowhere to go
            }
        }

        @Override
        public void onComplete() {
            try {
                parent.onCompleteCall.run();
            } catch (Throwable e) {
             // FIXME nowhere to go
            }
            
            actual.onComplete();

            try {
                parent.onAfterTerminateCall.run();
            } catch (Throwable e) {
             // FIXME nowhere to go
            }
        }
        
    }
}
