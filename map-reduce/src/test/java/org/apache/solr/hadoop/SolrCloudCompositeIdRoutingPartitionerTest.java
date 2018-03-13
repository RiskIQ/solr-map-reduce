package org.apache.solr.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

/**
 * @author Joe Linn
 * 03/09/2018
 */
public class SolrCloudCompositeIdRoutingPartitionerTest {
    private SolrCloudCompositeIdRoutingPartitioner partitioner;


    @Before
    public void setUp() throws Exception {
        partitioner = new SolrCloudCompositeIdRoutingPartitioner();
    }


    @Test
    public void testHashing() throws Exception {
        final int numPartitions = 64;
        Configuration conf = new Configuration();
        conf.set(SolrCloudCompositeIdRoutingPartitioner.SHARDS, "4");
        partitioner.setConf(conf);


        SolrInputDocumentWritable doc = new SolrInputDocumentWritable(new SolrInputDocument());

        assertThat(partitioner.getPartition(new Text("test"), doc, numPartitions), equalTo(3));
        assertThat(partitioner.getPartition(new Text("foobar"), doc, numPartitions), equalTo(13));
    }
}