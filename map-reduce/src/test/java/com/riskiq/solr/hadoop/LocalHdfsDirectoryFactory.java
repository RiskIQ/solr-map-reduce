package com.riskiq.solr.hadoop;

import org.apache.solr.core.HdfsDirectoryFactory;

/**
 * @author Joe Linn
 * 01/10/2020
 */
public class LocalHdfsDirectoryFactory extends HdfsDirectoryFactory {
    @Override
    public boolean isAbsolute(String path) {
        return super.isAbsolute(path) || path.startsWith("file:/");
    }
}
