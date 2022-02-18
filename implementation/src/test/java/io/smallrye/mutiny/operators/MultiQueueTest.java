package io.smallrye.mutiny.operators;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.operators.multi.MultiQueueOp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiQueueTest {

    // TODO Implement tests
    @Test
    public void testQueueingWithResultsAndCompletion() {
        Multi<Integer> multi = Multi.createFrom().items(1,2)
                .plug((m) -> new MultiQueueOp<>(m, 1));
        multi.subscribe().withSubscriber(AssertSubscriber.create())
                .assertSubscribed()
                .request(2)
                .awaitItems(2)
                .assertCompleted()
                .assertItems(1, 2);
    }

    @Test
    public void testQueueingWithFailure() {
        AtomicInteger count = new AtomicInteger();
        Multi<Integer> multi = Multi.createFrom().<Integer> emitter(emitter -> emitter.emit(count.incrementAndGet())
                        .emit(count.incrementAndGet())
                        .fail(new IOException("boom-" + count.incrementAndGet())))
                .plug((m) -> new MultiQueueOp<>(m, 1));

        multi.subscribe().withSubscriber(AssertSubscriber.create(2))
                .awaitItems(2)
                .assertItems(1, 2)
                .awaitFailure((throwable) -> new AssertionError("test"));
    }

    @Test
    public void testQueueingFailsOnSecondSubscriber() {
        AtomicInteger count = new AtomicInteger();
        Multi<Integer> multi = Multi.createFrom().deferred(() -> Multi.createFrom().items(count.incrementAndGet(),
                        count.incrementAndGet()))
                .plug((m) -> new MultiQueueOp<>(m, 1));

        multi.subscribe().withSubscriber(AssertSubscriber.create())
                .assertSubscribed();

        Throwable failure = multi.subscribe()
                .withSubscriber(AssertSubscriber.create())
                .awaitFailure()
                .getFailure();
        Assertions.assertNotNull(failure);
 }

    @Test
    void testQueueingWithFullQueue() {
        AtomicInteger count = new AtomicInteger();
        Multi<Integer> multi = Multi.createBy()
                .repeating().supplier(count::getAndIncrement).atMost(5)
                .plug((m) -> new MultiQueueOp<>(m, 1));

        AssertSubscriber<Integer> subscriber = multi.subscribe().withSubscriber(AssertSubscriber.create());

        subscriber.assertSubscribed()
                .request(1)
                .awaitItems(1);

        // Even though we have only requested 1 item, the second item should have been queued up
        // So the count should be 2. But only the second item because we only queued one.
        Assertions.assertEquals(2, count.get());

        subscriber.cancel();
     }

    //TODO can I test for race conditions?
}
