package io.smallrye.mutiny.operators;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.helpers.test.AssertionHelper;
import io.smallrye.mutiny.operators.multi.MultiQueueOp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiQueueTest {

    // TODO Implement tests
    @Test
    public void testQueueingWithResultsAndCompletion() {
        AtomicInteger count = new AtomicInteger();
        Multi<Integer> multi = Multi.createFrom().deferred(() -> Multi.createFrom().items(count.incrementAndGet(),
                        count.incrementAndGet()))
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

        Assertions.assertThrows(Exception.class, () -> multi.subscribe().withSubscriber(AssertSubscriber.create()).assertSubscribed());
    }

    //TODO can I test for race conditions?


    // TODO test examples from MultiCacheTEst
    //testCachingWithDeferredResult
}
