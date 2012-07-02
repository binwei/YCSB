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
package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

import java.util.Random;

import static java.lang.String.format;

/**
 * Generate integers resembling a hotspot distribution where x% of operations
 * access y% of data items. The parameters specify the bounds for the numbers,
 * the percentage of the of the interval which comprises the hot set and
 * the percentage of operations that access the hot set. Numbers of the hot set are
 * always smaller than any number in the cold set. Elements from the hot set and
 * the cold set are chose using a uniform distribution.
 *
 * @author sudipto
 */
public class HotspotIntegerGenerator extends IntegerGenerator {

    protected int lowerBound;
    protected int upperBound;
    protected int hotInterval;
    protected int coldInterval;
    protected double hotDataFraction;
    protected double hotOperationFraction;
    protected boolean init;

    /**
     * Create a generator for hotspot distributions.
     *
     * @param lowerBound        lower bound of the distribution.
     * @param upperBound        upper bound of the distribution.
     * @param hotDataFraction    percentage of data item
     * @param hotOperationFraction percentage of operations accessing the hot set.
     */
    public HotspotIntegerGenerator(int lowerBound, int upperBound,
                                   double hotDataFraction, double hotOperationFraction) {
        setLowerBound(lowerBound);
        setUpperBound(upperBound);
        setHotDataFraction(hotDataFraction);
        setHotOperationFraction(hotOperationFraction);
        init();
    }

    protected void init() {
        if (init) {
            if (lowerBound > upperBound) {
                throw new IllegalStateException(format("Lower bound is greater than upper bound %1$d > %2$d", lowerBound, upperBound));
            }
            int interval = upperBound - lowerBound;
            this.hotInterval = (int) (interval * hotDataFraction);
            this.coldInterval = interval - hotInterval;
            this.init = false;
        }
    }

    @Override
    public int nextInt() {
        init();
        Random random = Utils.random();
        int value;
        if (random.nextDouble() < hotOperationFraction) {
            // Choose a value from the hot set
            value = lowerBound + random.nextInt(hotInterval);
        } else {
            // Choose a value from the cold set
            value = lowerBound + hotInterval + random.nextInt(coldInterval);
        }
        setLastInt(value);
        return value;
    }

    /**
     * @return the lowerBound
     */
    public int getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(int lowerBound) {
        this.lowerBound = lowerBound;
        this.init = true;
    }

    /**
     * @return the upperBound
     */
    public int getUpperBound() {
        return upperBound;
    }

    public void setUpperBound(int upperBound) {
        this.upperBound = upperBound;
        this.init = true;
    }

    /**
     * @return the hotSetFraction
     */
    public double getHotDataFraction() {
        return hotDataFraction;
    }

    public void setHotOperationFraction(double hotOperationFraction) {
        if (hotOperationFraction < 0.0 || hotOperationFraction > 1.0) {
            throw new IllegalArgumentException("Hot operation fraction %1$f out of range [0, 1]" + hotOperationFraction);
        }
        this.hotOperationFraction = hotOperationFraction;
    }

    /**
     * @return the hotOpnFraction
     */
    public double getHotOperationFraction() {
        return hotOperationFraction;
    }

    public void setHotDataFraction(double hotDataFraction) {
        if (hotDataFraction < 0.0 || hotDataFraction > 1.0) {
            throw new IllegalArgumentException(format("Hot set fraction %1$f is out of range [0, 1]", hotDataFraction));
        }
        this.hotDataFraction = hotDataFraction;
        this.init = true;
    }

    @Override
    public double mean() {
        return hotOperationFraction * (lowerBound + hotInterval / 2.0)
                + (1 - hotOperationFraction) * (lowerBound + hotInterval + coldInterval / 2.0);
    }
}
