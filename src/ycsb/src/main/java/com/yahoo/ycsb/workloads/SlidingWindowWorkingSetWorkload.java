package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.WorkloadException;

import java.util.Properties;

public class SlidingWindowWorkingSetWorkload extends CoreWorkload {

    @Override
    public void init(Properties p) throws WorkloadException {
        super.init(p);
        // TODO: create sliding window
        // TODO: use ScrambledZipfianGenerator
    }
}
