package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.SlidingHotspotIntegerGenerator;

import static com.yahoo.ycsb.Utils.parseDouble;
import static com.yahoo.ycsb.Utils.parseInt;

public class SlidingHotspotWorkload extends CoreWorkload {

    public static final String DISTRIBUTION_SLIDING_HOTSPOT = "slidinghotspot";

    public static final String HOTSPOT_SLIDING_SPEED = "hotspotslidingspeed";

    /**
     * Default value for hotspot sliding speed in the items per second
     */
    public static final int HOTSPOT_SLIDING_SPEED_DEFAULT = 10;

    @Override
    protected IntegerGenerator createTransactionKeyGenerator() throws WorkloadException {
        String distribution = properties.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
        if (distribution.compareTo(DISTRIBUTION_SLIDING_HOTSPOT) == 0) {
            SlidingHotspotIntegerGenerator generator = createSlidingHotspotIntegerGenerator();
            generator.startSliding();
            return generator;
        } else {
            return super.createTransactionKeyGenerator();
        }
    }

    @Override
    public void cleanup() throws WorkloadException {
        super.cleanup();
        if (transactionKeyGenerator instanceof SlidingHotspotIntegerGenerator) {
            ((SlidingHotspotIntegerGenerator) transactionKeyGenerator).stopSliding();
        }
    }

    private SlidingHotspotIntegerGenerator createSlidingHotspotIntegerGenerator() {
        double dataFraction = parseDouble(properties.getProperty(HOTSPOT_DATA_FRACTION), HOTSPOT_DATA_FRACTION_DEFAULT);
        double operationFraction = parseDouble(properties.getProperty(HOTSPOT_OPERATION_FRACTION), HOTSPOT_OPERATION_FRACTION_DEFAULT);
        int slidingSpeed = parseInt(properties.getProperty(HOTSPOT_SLIDING_SPEED), HOTSPOT_SLIDING_SPEED_DEFAULT);
        return new SlidingHotspotIntegerGenerator(0, recordCount - 1, dataFraction, operationFraction, slidingSpeed);
    }
}
