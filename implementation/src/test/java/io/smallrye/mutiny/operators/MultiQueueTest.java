package io.smallrye.mutiny.operators;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

public class MultiQueueTest {

    // TODO Implement tests
    @Test
    public void testQueueingWithResultsAndCompletion() {
        AtomicInteger count = new AtomicInteger();
        Multi<Integer> multi = Multi.createFrom().deferred(() -> Multi.createFrom().items(count.incrementAndGet(),
                        count.incrementAndGet()))
                        .queue(1);
        multi.subscribe().withSubscriber(AssertSubscriber.create(2))
                .assertCompleted()
                .assertItems(1, 2);

        multi.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE))
                .assertCompleted()
                .assertItems(1, 2);
    }


    // TODO test examples from MultiCacheTEst
    //testCachingWithResultsAndCompletion
    //testCachingWithFailure
    //testCachingWithDeferredResult
}
