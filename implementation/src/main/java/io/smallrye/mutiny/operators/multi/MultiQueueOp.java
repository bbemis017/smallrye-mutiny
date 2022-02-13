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
     * The upstreamSubscription is used to control the flow of messages between it's Publisher (the upstream) and it's Subscriber (MultiQueueOp).
     */
    private Subscription upstreamSubscription;

    /**
     *  The multiQueueOpSubscription is used to control the flow of messages between it's Publisher (MultiQueueOp) and the MultiQueueOp's downstream subscriber.
     */
    private MultiQueueOpSubscription<T> multiQueueOpSubscription;


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
        // this is called when the downstream subscribes to the MultiQueueOp

        // The downstream of the MultiQueueOp subscribes to the MultiQueueOpSubscription
        MultiQueueOpSubscription<T> multiQueueOpSubscription = new MultiQueueOpSubscription<>(downstreamSubscriber, this);
        downstreamSubscriber.onSubscribe(multiQueueOpSubscription);
        this.multiQueueOpSubscription = multiQueueOpSubscription;

        // subscribe to the upstream of the MultiQueueOp
        this.upstream().subscribe().withSubscriber(this);
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
        this.upstreamSubscription = subscription;
    }

    @Override
    public void onNext(T t) {
        // called when the publisher publishes a new message
        this.multiQueueOpSubscription.send(t);
    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }


    /**
     * The {@link MultiQueueOpSubscription} is used to control the flow of messages between the
     * Publisher (MultiQueueOp) and the MultiQueueOp's downstream subscriber. This class should be instantiated and
     * passed to the downstream subscriber through the {@link io.smallrye.mutiny.subscription.MultiSubscriber}'s
     * onSubscribe method.
     *
     * @param <T> the type of item
     */
    static final class MultiQueueOpSubscription<T> implements Subscription {

        private final MultiQueueOp<T> multiQueueOp;

        /**
         * The MultiQueueOp's downstream subscriber.
         */
        private final MultiSubscriber<? super T> downstreamSubscriber;

        public MultiQueueOpSubscription(MultiSubscriber<? super T> downstreamSubscriber, MultiQueueOp<T> multiQueueOp) {
            this.multiQueueOp = multiQueueOp;
            this.downstreamSubscriber = downstreamSubscriber;
        }

        @Override
        public void request(long l) {
            this.multiQueueOp.upstreamSubscription.request(l);
        }

        public void send(T item) {
            downstreamSubscriber.onItem(item);
        }

        @Override
        public void cancel() {

        }
    }
}
