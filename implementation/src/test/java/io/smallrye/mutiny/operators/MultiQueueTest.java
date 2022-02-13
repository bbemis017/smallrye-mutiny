package io.smallrye.mutiny.operators;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.helpers.test.AssertSubscriber;
import io.smallrye.mutiny.operators.multi.MultiQueueOp;
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

    @Test
    public void testQueueingWithResultsAndCompletion2() {
        AtomicInteger count = new AtomicInteger();
        Multi<Integer> multi = Multi.createFrom().deferred(() -> Multi.createFrom().items(count.incrementAndGet(),
                        count.incrementAndGet()))
                .plug(MultiQueueOp::new);
        multi.subscribe().withSubscriber(AssertSubscriber.create())
                .assertSubscribed()
                .request(2)
                .awaitItems(2)
                .assertCompleted()
                .assertItems(1, 2);

        //TODO not really sure if we need to support multiple subscribers? hmm
//        multi.subscribe().withSubscriber(AssertSubscriber.create(Long.MAX_VALUE))
//                .assertCompleted()
//                .assertItems(1, 2);
    }

    //TODO can I test for race conditions?


    // TODO test examples from MultiCacheTEst
    //testCachingWithResultsAndCompletion
    //testCachingWithFailure
    //testCachingWithDeferredResult
}
