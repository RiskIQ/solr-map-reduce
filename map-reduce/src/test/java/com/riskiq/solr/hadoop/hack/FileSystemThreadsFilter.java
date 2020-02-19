package com.riskiq.solr.hadoop.hack;

import com.carrotsearch.randomizedtesting.ThreadFilter;

/**
 * @author Joe Linn
 * 03/16/2018
 */
public class FileSystemThreadsFilter implements ThreadFilter {
    @Override
    public boolean reject(Thread t) {
        return "org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner".equals(t.getName());
    }
}
