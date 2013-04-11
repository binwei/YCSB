package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class SlidingHotspotIntegerGenerator extends HotspotIntegerGenerator {

    private static class DaemonThreadFactory implements ThreadFactory {

        public Thread newThread(Runnable runnable) {
            Thread thread = new Thread(runnable);
            thread.setDaemon(true);
            if (thread.getPriority() != Thread.NORM_PRIORITY) {
                thread.setPriority(Thread.NORM_PRIORITY);
            }
            return thread;
        }
    }

    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory());
    private ScheduledFuture<?> slideHotspotTaskFuture;

    protected int slidingSpeed;
    protected int slideWithinBoundsBy;
    protected int slidesCount;

    public SlidingHotspotIntegerGenerator(int lowerBound, int upperBound,
                                          double hotDataFraction, double hotOperationFraction, int slidingSpeed) {
        super(lowerBound, upperBound, hotDataFraction, hotOperationFraction);
        this.slidingSpeed = slidingSpeed;
    }

    public int getSlidingSpeed() {
        return slidingSpeed;
    }

    public void setSlidingSpeed(int slidingSpeed) {
        this.slidingSpeed = slidingSpeed;
    }

    public void startSliding() {
        if (slideHotspotTaskFuture != null) {
            throw new IllegalStateException("Sliding task is already started");
        }
        slideHotspotTaskFuture = executor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                slideNext();
            }
        }, 1000, 1000, TimeUnit.MILLISECONDS);
    }

    public void stopSliding() {
        if (slideHotspotTaskFuture != null) {
            slideHotspotTaskFuture.cancel(true);
            slideHotspotTaskFuture = null;
        }
    }

    public void slideNext() {
        slidesCount++;
        slide();
    }

    private void slide() {
        int slideBy = slidesCount * slidingSpeed;
        int slideBounds = coldInterval;
        int slideWithinBoundsBy;
        // sliding hotspot forward
        if ((slideBy / slideBounds % 2) == 0) {
            slideWithinBoundsBy = slideBy % slideBounds;
        } else if (slidingSpeed != 0) {
            int slidesCountWithinBounds = slideBounds / slidingSpeed;
            slideWithinBoundsBy = (slidesCountWithinBounds - (slidesCount % slidesCountWithinBounds)) * slidingSpeed;
        } else {
            slideWithinBoundsBy = 0;
        }
        this.slideWithinBoundsBy = slideWithinBoundsBy;
    }

    @Override
    public int nextInt() {
        init();
        Random random = Utils.random();
        int value;
        if (random.nextDouble() < hotOperationFraction) {
            // choose a value from the hot set
            value = lowerBound + slideWithinBoundsBy + random.nextInt(hotInterval);
        } else {
            // choose a value from the cold set
            value = lowerBound + hotInterval + random.nextInt(coldInterval - slideWithinBoundsBy);
        }
        setLastInt(value);
        return value;
    }

    public static void main(String[] args) {
        SlidingHotspotIntegerGenerator generator = new SlidingHotspotIntegerGenerator(0, 100, 0.5, 1, 10);
        int length = 1000;
        for (int i = 0; i < 20; i++) {
            generator.slideNext();
            generate(generator, length);
        }
    }

    public static void generate(IntegerGenerator generator, int length) {
        int min = generator.nextInt();
        int max = generator.lastInt();
        for (int i = 0; i < length; generator.nextInt(), i++) {
            int value = generator.lastInt();
            if (value < min) {
                min = value;
            }
            if (value > max) {
                max = value;
            }
        }
        System.out.println(String.format("min=%1$d, max=%2$d", min, max));
    }
}
