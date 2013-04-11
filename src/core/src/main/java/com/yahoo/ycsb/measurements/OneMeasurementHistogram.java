/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb.measurements;

import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 *
 * @author cooperb
 */
public class OneMeasurementHistogram extends OneMeasurement {

    public static final String BUCKETS = "histogram.buckets";
    public static final String BUCKETS_DEFAULT = "1000";
    public static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");

    int buckets;
    int[] histogram;
    int histogramOverflow;
    int operations;
    long totalLatency;

    //keep a windowed version of these stats for printing status
    int windowOperations;
    long windowTotalLatency;

    int min;
    int max;
    Map<Integer, AtomicInteger> returnCodes;
    Map<Integer, LatencyPerBucket> latencyPerBucketMap;

    class LatencyPerBucket {
        double totalLatency;
        int measurements;

        public void measure(int latency) {
            totalLatency += latency;
            measurements++;
        }

        public double getAverageLatency() {
            return totalLatency / measurements;
        }
    }

    public OneMeasurementHistogram(String name, Properties props) {
        super(name);
        buckets = Integer.parseInt(props.getProperty(BUCKETS, BUCKETS_DEFAULT));
        histogram = new int[buckets];
        histogramOverflow = 0;
        operations = 0;
        totalLatency = 0;
        windowOperations = 0;
        windowTotalLatency = 0;
        min = -1;
        max = -1;
        returnCodes = new HashMap<Integer, AtomicInteger>();
        latencyPerBucketMap = new HashMap<Integer, LatencyPerBucket>();
    }

    /* (non-Javadoc)
      * @see com.yahoo.ycsb.OneMeasurement#reportReturnCode(int)
      */
    public synchronized void reportReturnCode(int code) {
        AtomicInteger count = returnCodes.get(code);
        if (count == null) {
            returnCodes.put(code, count = new AtomicInteger());
        }
        count.incrementAndGet();
    }


    /* (non-Javadoc)
      * @see com.yahoo.ycsb.OneMeasurement#measure(int)
      */
    public synchronized void measure(int latency) {
        int bucket = latency / 1000;
        if (bucket >= buckets) {
            histogramOverflow++;
        } else {
            histogram[bucket]++;
            LatencyPerBucket latencyPerBucket = latencyPerBucketMap.get(bucket);
            if (latencyPerBucket == null) {
                latencyPerBucketMap.put(bucket, latencyPerBucket = new LatencyPerBucket());
            }
            latencyPerBucket.measure(latency);
        }
        operations++;
        totalLatency += latency;
        windowOperations++;
        windowTotalLatency += latency;

        if ((min < 0) || (latency < min)) {
            min = latency;
        }

        if ((max < 0) || (latency > max)) {
            max = latency;
        }
    }

    @Override
    public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
        exporter.write(getName(), "Operations", operations);
        exporter.write(getName(), "AverageLatency(us)", (((double) totalLatency) / ((double) operations)));
        exporter.write(getName(), "MinLatency(us)", min);
        exporter.write(getName(), "MaxLatency(us)", max);

        int counter = 0;
        boolean done95th = false;
        for (int bucket = 0; bucket < buckets; bucket++) {
            counter += histogram[bucket];
            if ((!done95th) && (((double) counter) / ((double) operations) >= 0.95)) {
                exporter.write(getName(), "95thPercentileLatency(us)", latencyPerBucketMap.get(bucket).getAverageLatency());
                done95th = true;
            }
            if (((double) counter) / ((double) operations) >= 0.99) {
                exporter.write(getName(), "99thPercentileLatency(us)", latencyPerBucketMap.get(bucket).getAverageLatency());
                break;
            }
        }
        for (Map.Entry<Integer, AtomicInteger> entry : returnCodes.entrySet()) {
            exporter.write(getName(), "Return=" + entry.getKey(), entry.getValue().get());
        }
        for (int i = 0; i < buckets; i++) {
            if (histogram[i] > 0) {
                exporter.write(getName(), Integer.toString(i), histogram[i]);
            }
        }
        exporter.write(getName(), ">" + buckets, histogramOverflow);
    }

    @Override
    public String getSummary() {
        if (windowOperations == 0) {
            return "";
        }
        double report = ((double) windowTotalLatency) / ((double) windowOperations);
        windowTotalLatency = 0;
        windowOperations = 0;
        return "[" + getName() + " AverageLatency(us)=" + DECIMAL_FORMAT.format(report) + "]";
    }
}
