package io.smallrye.mutiny.operators.multi;


import io.smallrye.mutiny.Context;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.Subscriptions;
import io.smallrye.mutiny.subscription.ContextSupport;
import io.smallrye.mutiny.subscription.MultiSubscriber;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@code multi} queueing a limited number of events emitted from upstreams and replaying it to subscribers.
 *
 * @param <T> the type of item
 */
@SuppressWarnings("SubscriberImplementation")
public class MultiQueueOp2<T> extends AbstractMultiOperator<T, T> implements Subscriber<T>, ContextSupport {

    /**
     * Stores whether we already subscribed to the upstream.
     */
    private final AtomicBoolean hasSubscribedToUpstream = new AtomicBoolean();

    private Subscription upstreamSubscription;

    /**
     * Max number of events to queue at one time.
     */
    private final int maxQueueSize;

    /**
     * The current set of downstream subscribers.
     */
    private QueueSubscription<T> subscriber;
    private volatile boolean terminated;

    /**
     * Stores a limited number of events at a time. Queue follows FIFO
     */
    private final BlockingQueue<T> eventQueue;


    private volatile Context context;

    /**
     * If the upstream has terminated with a failure, this stores the failure.
     */
    private Throwable failure;

    /**
     * {@code true} if the source has terminated.
     */
    private volatile boolean done;

    public MultiQueueOp2(Multi<? extends T> upstream, final int maxQueueSize) {
        super(upstream);
        this.maxQueueSize = maxQueueSize;
        this.eventQueue = new LinkedBlockingQueue<>(this.maxQueueSize);
    }

    public boolean hasNext() {
        return eventQueue.remainingCapacity() < this.maxQueueSize;
    }

    /**
     * =======================================================================
     * ======= Override AbstractMultiOp Functionality
     * ========================================================================
     */

    @Override
    public void subscribe(MultiSubscriber<? super T> downstream) {
        System.out.println("subscribe");
        // create new QueueSubscription consumer
        QueueSubscription consumer = new QueueSubscription<>(downstream, this);

        // The downstream of the MultiQueueOp subscribes to the QueueSubscription
        downstream.onSubscribe(consumer);

        // this (the MultiQueueOp) gets a downstream subscription which is the QueueSubscription
        addDownstreamSubscription(consumer);

        if(hasSubscribedToUpstream.compareAndSet(false, true)) {
            if (downstream instanceof ContextSupport) {
                this.context = ((ContextSupport) downstream).context();
            } else {
                this.context = Context.empty();
            }
            // subscribe to the upstream of the MultiQueueOp
            upstream.subscribe().withSubscriber(this);
        } else {
            // TODO don't think I understand why we would do this
            consumer.popRequested();
        }
        System.out.println("subscribe done");
    }

    public synchronized void requestToRefillQueue() {
        this.upstreamSubscription.request(this.eventQueue.remainingCapacity());
    }

    private synchronized void addDownstreamSubscription(QueueSubscription<T> consumer) {
        if (terminated) {
            return;
        }
        this.subscriber = consumer;
    }

    private synchronized  void addUpstreamSubscription(Subscription upstreamSubscription) {
        if (terminated) {
            return;
        }
        this.upstreamSubscription = upstreamSubscription;
    }

    private synchronized void remove(QueueSubscription<T> consumer) {
        subscriber = null;
    }

    /**
     * =======================================================================
     * ======= Override Subscriber Functionality
     * ========================================================================
     */

    @Override
    public void onSubscribe(Subscription subscription) {
        this.addUpstreamSubscription(subscription);
        this.requestToRefillQueue();
    }

    @Override
    public synchronized void onNext(T t) {
        // so technically if the eventQueue is full this will block until it is emptied. But I think in theory onNext
        // will only get called if items are requested. So as long as we don't request more items when the queue is full
        // this should be ok
        this.eventQueue.add(t);

        // tell the subscriber to grab data
        this.subscriber.popRequested();

        if (this.eventQueue.remainingCapacity() > 0) {
            //TODO would it make sense to request more at this point, hmmm not 100% sure where that goes!
//            this.subscriber.request(this.eventQueue.remainingCapacity());
        }
    }

    @Override
    public void onError(Throwable throwable) {
        if(this.done) {
            return;
        }
        this.failure = throwable;
        this.done = true;
        this.terminated = true;
        this.subscriber.popRequested();
    }

    @Override
    public void onComplete() {
        done = true;
        terminated = true;
        this.subscriber.popRequested();
    }

    @Override
    public Context context() {return this.context;}


    /**
     * Hosts the downstream consumer and its current requested states.
     * @param <T> - the type of item.
     */
    static final class QueueSubscription<T> implements Subscription {

        private final MultiSubscriber<? super T> downstream;
        private final MultiQueueOp2<T> queue;
        private final AtomicLong requested = new AtomicLong();
        private final AtomicInteger wip = new AtomicInteger();

        QueueSubscription(MultiSubscriber<? super T> downstream, MultiQueueOp2<T> queue) {
            this.downstream = downstream;
            this.queue = queue;
        }

        @Override
        public void request(long n) {
            System.out.println("request " + n);
            if (n > 0) {
                Subscriptions.add(requested, n);
                popRequested();
//                this.queue.requestToRefillQueue();
            }
        }

        public void popRequested() {
            if (this.wip.getAndIncrement() != 0) {
                return; // TODO why do we skip if wip != 0, is this a lock?
            }
            int missed = 1;

            for(;;) {
                System.out.println("check 1");

                // if the queue is done and there are no events remaining
                if(this.queue.done && !this.queue.hasNext()) {
                    if (this.queue.failure != null) {
                        // pass the failure from the MultiQueueOp to the MultiQueueOp's downstream subscriber
                        this.downstream.onError(this.queue.failure);
                    } else {
                        this.downstream.onCompletion();
                    }

                    return;
                }

                System.out.println("check 2");

                long consumerRequested = requested.get();
                if (consumerRequested == Long.MIN_VALUE) { // if cancelled, TODO why does this mean cancelled?
                    return;
                }

                System.out.println("check 3");


                if (consumerRequested > 0L && this.queue.hasNext()) {
                    try {
                        T event = this.queue.eventQueue.take();
                        downstream.onItem(event);
                    } catch (InterruptedException e) {
                        // tell the MultiQueueOp that an error occurred. This should clean things up properly between the
                        // upstream and downstream in theory.
                        System.out.println("Interrupted exception");
                        this.queue.onError(e);
                    } finally {
                        System.out.println("request fulfilled, remaining QueueSize" + this.queue.eventQueue.remainingCapacity());
                        Subscriptions.subtract(this.requested, 1);
                        this.queue.requestToRefillQueue();
                        continue;
                    }
                } else if (consumerRequested > 0L && this.queue.eventQueue.remainingCapacity() > 0){
                    System.out.println("MultiQueueOp needs to request more from upstream");
                }

                System.out.println("check 4");

                // this is kinda interesting, so if the capacity is not there yet or the consumer request has not been updated
                // yet then we just repeat until we have caught up it
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
            System.out.println("check 5");
        }

        @Override
        public void cancel() {
            // remove requests if there are any
            if (requested.getAndSet(Long.MIN_VALUE) != Long.MIN_VALUE) {
                // If there are active requests remove the QueueSubscription from the MultiQueueOp's subscribers
                queue.remove(this);
            }
        }
    }
}

