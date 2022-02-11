package io.smallrye.mutiny.operators.multi;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

// TODO bbemis it might be a good exercise to implement a simpler MultiOperator first that just passes everything along
// nothing fancy
/**
 * NOTES:
 *  - reactive streams: https://www.baeldung.com/java-9-reactive-streams
 */

/**
 * A {@code multi} queueing a limited number of events emitted from upstreams and replaying it to subscribers.
 *
 * @param <T> the type of item
 */
public class MultiQueueOp<T> extends AbstractMultiOperator<T, T> implements  Subscriber<T> {


    /**
     * =========================================================================
     * =================== Overriding AbstractMultiOperator ====================
     * =================== These are the Publisher methods  ====================
     * =========================================================================
     */

    public MultiQueueOp(Multi<? extends T> upstream) {
        super(upstream);
    }

    @Override
    public void subscribe(MultiSubscriber<? super T> downstreamSubscriber) {
//        super.subscribe(downstreamSubscriber);
        // this is called when the downstream subscribes to the MultiQueueOp
    }

    /**
     * =========================================================================
     * =================== Overriding Subscriber Interface  ====================
     * =========================================================================
     */

    @Override
    public void onSubscribe(Subscription subscription) {
        // is called before processing starts when MultiQueueOp subscribes to the upstream.
        // the subscription is used to control the flow of messages between the Subscriber (MultiQueueOp) and the Publisher (the upstream).

    }

    @Override
    public void onNext(T t) {
        // called when the publisher publishes a new message

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }


    /**
     * The {@link MultiQueueOp2.QueueSubscription} is used to control the flow of messages between the
     * Publisher (MultiQueueOp) and the MultiQueueOp's downstream subscriber. This class should be instantiated and
     * passed to the downstream subscriber through the {@link io.smallrye.mutiny.subscription.MultiSubscriber}'s
     * onSubscribe method.
     *
     * @param <T> the type of item
     */
    static final class QueueSubscription<T> implements Subscription {

        @Override
        public void request(long l) {

        }

        @Override
        public void cancel() {

        }
    }
}
