package org.apache.solr.hadoop;

import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

/**
 * @author Joe Linn
 * 01/10/2020
 */
public class SolrMergeDriverTest extends IndexingTestCase {
    @Test
    public void testMergeIndex() throws Exception {
        // write data
        File inputDir = temporaryFolder.newFolder("input");
        writeInputFile(new File(inputDir, "part-r-00000.txt"), 0, 50);
        writeInputFile(new File(inputDir, "part-r-00001.txt"), 50, 50);

        // set up Solr configs
        File solrHomeDir = temporaryFolder.newFolder("solrHome");
        copySolrConfigs(solrHomeDir);

        // create an index
        File outputDir = new File(temporaryFolder.getRoot(), "output");

        final int numReducers = 3;
        ToolRunner.run(getConf(), new SolrIndexDriver(), new String[]{
                "-Dmapreduce.job.map.class=org.apache.solr.hadoop.IndexingTestCase$TestSolrMapper",
                "-Dmapreduce.job.partitioner.class=org.apache.solr.hadoop.SolrCloudCompositeIdRoutingPartitioner",
                "-Dsolr.record.writer.batch.size=20",
                "--solr-home-dir", "file://" + solrHomeDir.getAbsolutePath(),
                "--shards", "1",
                "--reducers", String.valueOf(numReducers),
                "--output-dir", outputDir.getAbsolutePath(),
                "-i", inputDir.getAbsolutePath()
        });

        assertThat(outputDir.exists(), equalTo(true));
        File reducersDir = new File(outputDir, "reducers");
        assertThat(reducersDir.exists(), equalTo(true));
        for (int i = 0; i < numReducers; i++) {
            File partDir = new File(reducersDir, "part-r-0000" + i);
            assertThat(partDir.exists(), equalTo(true));
            assertThat(partDir.isDirectory(), equalTo(true));
        }

        // merge the index
        ToolRunner.run(getConf(), new SolrMergeDriver(), new String[]{
                "-DmaxSegmentsOnTreeMerge=1",
                "--shards", "1",
                "--reducers", String.valueOf(numReducers),
                "--input-dir", outputDir.getAbsolutePath(),
        });

        File mergeOut1Dir = new File(outputDir, "mtree-merge-input-iteration1");
        assertThat(mergeOut1Dir.exists(), equalTo(true));
        File merge1InputList = new File(mergeOut1Dir, "input-list.txt");
        assertThat(merge1InputList.exists(), equalTo(true));
        List<String> merge1InputLines = Files.readAllLines(merge1InputList.toPath());
        assertThat(merge1InputLines, hasSize(numReducers));

        File resultsDir = new File(outputDir, "results");
        assertThat("results directory should exist", resultsDir.exists(), equalTo(true));
        File mergedIndexDir = new File(resultsDir, "part-00000/data/index");
        try (MMapDirectory mmap = new MMapDirectory(mergedIndexDir.toPath());) {
            SearcherFactory searcherFactory = new SearcherFactory();
            SearcherManager searcherManager = new SearcherManager(mmap, searcherFactory);
            TopDocs topDocs = searcherManager.acquire().search(new MatchAllDocsQuery(), 100);
            assertThat(topDocs.totalHits, equalTo((long) 100));
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(mmap);
            assertThat(segmentInfos.size(), equalTo(1));
        }
    }
}