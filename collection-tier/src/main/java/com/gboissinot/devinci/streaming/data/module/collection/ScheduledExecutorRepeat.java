package com.gboissinot.devinci.streaming.data.module.collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * @author Gregory Boissinot
 */
class ScheduledExecutorRepeat {

    private static final Logger logger = LoggerFactory.getLogger(ScheduledExecutorRepeat.class);

    private final Collector collector;
    private final CountDownLatch latch;

    ScheduledExecutorRepeat(Collector collector, int maxRepeat) {
        this.collector = collector;
        this.latch = new CountDownLatch(maxRepeat);
    }

    void repeat() throws InterruptedException {
        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        SchedulingTask scheduledTask = new SchedulingTask(latch);
        ScheduledFuture<?> scheduledFuture = executorService.scheduleWithFixedDelay(scheduledTask, 1, 30, TimeUnit.SECONDS);
        latch.await();
        scheduledFuture.cancel(true);
        executorService.shutdown();
    }

    private class SchedulingTask implements Runnable {

        private final CountDownLatch latch;

        private SchedulingTask(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                logger.info("Launching collector");
                collector.collect();
                latch.countDown();
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }
    }
}
