package com.riskiq.solr.hadoop;

import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Test;

import java.io.File;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * @author Joe Linn
 * 01/08/2020
 */
public class SolrIndexDriverTest extends IndexingTestCase {
    @Test
    public void testCreateIndex() throws Exception {
        // write some data
        File inputDir = temporaryFolder.newFolder("input");
        final int numDocs = 50;
        writeInputFile(new File(inputDir, "part-r-00000.txt"), 0, numDocs);

        // set up Solr configs
        File solrHomeDir = temporaryFolder.newFolder("solrHome");
        copySolrConfigs(solrHomeDir);

        // create an index
        File outputDir = new File(temporaryFolder.getRoot(), "output");

        ToolRunner.run(getConf(), new SolrIndexDriver(), new String[]{
                "-Dmapreduce.job.map.class=com.riskiq.solr.hadoop.IndexingTestCase$TestSolrMapper",
                "-Dmapreduce.job.partitioner.class=com.riskiq.solr.hadoop.SolrCloudCompositeIdRoutingPartitioner",
                "-Dsolr.record.writer.batch.size=20",
                "--solr-home-dir", "file://" + solrHomeDir.getAbsolutePath(),
                "--shards", "1",
                "--reducers", "1",
                "--output-dir", outputDir.getAbsolutePath(),
                "-i", inputDir.getAbsolutePath()
        });

        assertThat(outputDir.exists(), equalTo(true));
        File reducersDir = new File(outputDir, "reducers");
        assertThat(reducersDir.exists(), equalTo(true));
        File part0Dir = new File(reducersDir, "part-r-00000");
        assertThat(part0Dir.exists(), equalTo(true));
        assertThat(part0Dir.isDirectory(), equalTo(true));

        try (MMapDirectory mmap = new MMapDirectory(new File(part0Dir, "data/index").toPath());) {
            SearcherFactory searcherFactory = new SearcherFactory();
            SearcherManager searcherManager = new SearcherManager(mmap, searcherFactory);
            TopDocs topDocs = searcherManager.acquire().search(new MatchAllDocsQuery(), numDocs);
            assertThat(topDocs.totalHits.value, equalTo((long) numDocs));
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(mmap);
            assertThat(segmentInfos.size(), equalTo(1));
        }
    }


}