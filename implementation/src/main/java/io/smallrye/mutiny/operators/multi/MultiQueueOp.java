package io.smallrye.mutiny.operators.multi;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

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
     * Stores a limited number of events at a time. Queue follows FIFO
     */
    private final BlockingQueue<T> eventQueue;

    /**
     * The upstreamSubscription is used to control the flow of messages between it's Publisher (the upstream) and it's Subscriber (MultiQueueOp).
     */
    private Subscription upstreamSubscription;

    /**
     *  The multiQueueOpSubscription is used to control the flow of messages between it's Publisher (MultiQueueOp) and the MultiQueueOp's downstream subscriber.
     */
    private MultiQueueOpSubscription<T> multiQueueOpSubscription;

    /**
     * {@code true} if the multiQueueOp's upstream has completed.
     */
    private boolean upstreamIsComplete; // TODO: https://www.geeksforgeeks.org/volatile-keyword-in-java/#:~:text=For%20Java%2C%20%E2%80%9Cvolatile%E2%80%9D%20tells,scope%20of%20the%20program%20itself.

    private Throwable failure;

    /**
     * =========================================================================
     * =================== Overriding AbstractMultiOperator ====================
     * =================== These are the Publisher methods  ====================
     * =========================================================================
     */

    public MultiQueueOp(Multi<? extends T> upstream, final int maxQueueSize) {
        super(upstream);
        this.eventQueue = new LinkedBlockingQueue<>(maxQueueSize);
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

    public void requestToRefillQueue() {
        this.upstreamSubscription.request(this.eventQueue.remainingCapacity());
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

        this.requestToRefillQueue();
    }

    @Override
    public void onNext(T t) {
        System.out.println("MultiQueueOp#onNext");
        // called when the publisher publishes a new message
        this.eventQueue.add(t);

        this.multiQueueOpSubscription.drainQueue();
    }

    @Override
    public void onError(Throwable throwable) {
        if (this.upstreamIsComplete) {
            return;
        }

        this.failure = throwable;
        this.upstreamIsComplete = true;
        this.multiQueueOpSubscription.sendCompletion();
    }

    @Override
    public void onComplete() {
        System.out.println("MultiQueueOp#onComplete start");
        this.upstreamIsComplete = true;
        this.multiQueueOpSubscription.sendCompletion();
        System.out.println("MultiQueueOp#onComplete end");
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

        private final AtomicLong requestedItems = new AtomicLong();

        public MultiQueueOpSubscription(MultiSubscriber<? super T> downstreamSubscriber, MultiQueueOp<T> multiQueueOp) {
            this.multiQueueOp = multiQueueOp;
            this.downstreamSubscriber = downstreamSubscriber;
        }

        @Override
        public void request(long numOfItems) {
            if (numOfItems <= 0 || this.multiQueueOp.failure != null) return;

            Subscriptions.add(this.requestedItems, numOfItems);
            drainQueue();
        }

        public void sendCompletion() {
            if (this.multiQueueOp.failure != null) {
                this.downstreamSubscriber.onError(this.multiQueueOp.failure);
                return;
            }

            if (this.multiQueueOp.upstreamIsComplete) {
                System.out.println("MultiQueueOpSubscription#sendCompletion completion");
                this.downstreamSubscriber.onCompletion();
            }
        }

        public void drainQueue() {
            System.out.println("MultiQueueOpSubscription#drainQueue");

            // if the upstream is complete and there are no events remaining
            if(this.multiQueueOp.upstreamIsComplete && this.multiQueueOp.eventQueue.remainingCapacity() < 1) {
                if (this.multiQueueOp.failure != null) {
                    // pass the failure from the MultiQueueOp to the MultiQueueOp's downstream subscriber
                    this.downstreamSubscriber.onError(this.multiQueueOp.failure);
                } else {
                    this.downstreamSubscriber.onCompletion();
                }
                return;
            }
            System.out.println("Check 1");

            long consumerRequested = this.requestedItems.get();
            System.out.println("req; " + consumerRequested + " cap: " + this.multiQueueOp.eventQueue.remainingCapacity());
            if (consumerRequested == Long.MIN_VALUE) { // if cancelled
                return;
            } else if(consumerRequested > 0L && this.multiQueueOp.eventQueue.remainingCapacity() < this.multiQueueOp.eventQueue.size()) {
                System.out.println("MulitQueueOpSubscription#drainQueue request > 0 && queueRemainingCapacity > 0");
                try {
                    T event = this.multiQueueOp.eventQueue.take();
                    this.downstreamSubscriber.onItem(event);
                } catch (InterruptedException e) {
                    // tell the MultiQueueOp that an error occurred. This should clean things up properly between the
                    // upstream and downstream in theory.
                    System.out.println("Interrupted exception");
                    this.multiQueueOp.onError(e); //TODO could this end up looping if onError calls drain?
                } finally {
                    System.out.println("request fulfilled, remaining QueueSize" + this.multiQueueOp.eventQueue.remainingCapacity());
                    Subscriptions.subtract(this.requestedItems, 1);
                    this.multiQueueOp.requestToRefillQueue();
//                    continue;
                }
            }

        }

        @Override
        public void cancel() {

        }
    }
}
