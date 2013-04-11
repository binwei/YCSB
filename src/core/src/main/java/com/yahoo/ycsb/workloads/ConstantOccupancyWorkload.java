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
package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.WorkloadException;
import com.yahoo.ycsb.generator.IntegerGenerator;

import static com.yahoo.ycsb.Client.*;
import static com.yahoo.ycsb.Utils.parseInt;

/**
 * A disk-fragmenting workload.
 * <p>
 * Properties to control the client:
 * </p>
 * <UL>
 * <LI><b>disksize</b>: how many bytes of storage can the disk store? (default 100,000,000)
 * <LI><b>occupancy</b>: what fraction of the available storage should be used? (default 0.9)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate on - uniform, zipfian or latest (default: histogram)
 * </ul>
 * <p/>
 * <p/>
 * <p> See also:
 * Russell Sears, Catharine van Ingen.
 * <a href='https://database.cs.wisc.edu/cidr/cidr2007/papers/cidr07p34.pdf'>Fragmentation in Large Object Repositories</a>,
 * CIDR 2006. [<a href='https://database.cs.wisc.edu/cidr/cidr2007/slides/p34-sears.ppt'>Presentation</a>]
 * </p>
 *
 * @author sears
 */
public class ConstantOccupancyWorkload extends CoreWorkload {

    public static final String STORAGE_AGE_PROPERTY = "storageages";
    public static final long STORAGE_AGE_PROPERTY_DEFAULT = 10;

    public static final String DISK_SIZE_PROPERTY = "disksize";
    public static final long DISK_SIZE_PROPERTY_DEFAULT = 100 * 1000 * 1000;

    public static final String OCCUPANCY_PROPERTY = "occupancy";
    public static final double OCCUPANCY_PROPERTY_DEFAULT = 0.9;

    @Override
    public void init() throws WorkloadException {
        long diskSize = Long.parseLong(properties.getProperty(DISK_SIZE_PROPERTY, DISK_SIZE_PROPERTY_DEFAULT + ""));
        long storageAges = Long.parseLong(properties.getProperty(STORAGE_AGE_PROPERTY, STORAGE_AGE_PROPERTY_DEFAULT + ""));
        double occupancy = Double.parseDouble(properties.getProperty(OCCUPANCY_PROPERTY, OCCUPANCY_PROPERTY_DEFAULT + ""));

        if ((properties.getProperty(RECORD_COUNT_PROPERTY) != null) ||
                (properties.getProperty(INSERT_COUNT_PROPERTY) != null) ||
                (properties.getProperty(OPERATION_COUNT_PROPERTY) != null)) {
            System.err.println("Warning: record, insert or operation count was set prior to init ConstantOccupancyWorkload. Overriding old values");
        }
        IntegerGenerator g = CoreWorkload.createFieldLengthGenerator(properties);
        double fieldSize = g.mean();
        int fieldCount = parseInt(properties.getProperty(FIELD_COUNT_PROPERTY), FIELD_COUNT_PROPERTY_DEFAULT);
        long objectCount = (long) (occupancy * ((double) diskSize / (fieldSize * (double) fieldCount)));
        if (objectCount == 0) {
            throw new IllegalStateException("Object count was zero.  Perhaps disksize is too low?");
        }
        properties.setProperty(RECORD_COUNT_PROPERTY, Long.toString(objectCount));
        properties.setProperty(OPERATION_COUNT_PROPERTY, Long.toString(storageAges * objectCount));
        properties.setProperty(INSERT_COUNT_PROPERTY, Long.toString(objectCount));
        super.init();
    }
}
