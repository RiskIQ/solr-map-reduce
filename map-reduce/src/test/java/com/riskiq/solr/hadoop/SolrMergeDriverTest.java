package com.riskiq.solr.hadoop;

import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.MMapDirectory;
import org.apache.solr.util.FileUtils;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

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

        final int numReducers = 10;
        ToolRunner.run(getConf(), new SolrIndexDriver(), new String[]{
                "-Dmapreduce.job.map.class=com.riskiq.solr.hadoop.IndexingTestCase$TestSolrMapper",
                "-Dmapreduce.job.partitioner.class=com.riskiq.solr.hadoop.SolrCloudCompositeIdRoutingPartitioner",
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

        File mergeConfigDir = temporaryFolder.newFolder("mergeConfig");
        File confDir = Files.createDirectory(new File(mergeConfigDir, "conf").toPath()).toFile();
        final String solrMergeConfigFile = "solrconfig_merge.xml";
        File mergeConfigFile = new File(confDir, solrMergeConfigFile);
        FileUtils.copyFile(new File(getClass().getResource(solrMergeConfigFile).toURI()), mergeConfigFile);

        // merge the index
        ToolRunner.run(getConf(), new SolrMergeDriver(), new String[]{
                "-DmaxSegmentsOnTreeMerge=1",
                "--shards", "1",
                "--reducers", String.valueOf(numReducers),
                "--input-dir", outputDir.getAbsolutePath(),
                "--solr-home-dir", "file://" + mergeConfigDir.getAbsolutePath(),
                "--solr-config-file-name", solrMergeConfigFile,
                "--fanout", Integer.toString(numReducers / 2)
        });

        File mergeOut1Dir = new File(outputDir, "mtree-merge-input-iteration1");
        assertThat(mergeOut1Dir.exists(), equalTo(true));
        File merge1InputList = new File(mergeOut1Dir, "input-list.txt");
        assertThat(merge1InputList.exists(), equalTo(true));
        List<String> merge1InputLines = Files.readAllLines(merge1InputList.toPath());
        assertThat(merge1InputLines, hasSize(numReducers));

        File iterationFile = new File(outputDir, "_ITERATION");
        assertThat(iterationFile.exists(), equalTo(true));

        List<String> iterationLines = Files.readAllLines(iterationFile.toPath());
        assertThat(iterationLines.get(0), equalTo("3"));
        assertThat(iterationLines.get(1), equalTo("0"));

        File resultsDir = new File(outputDir, "results");
        assertThat("results directory should exist", resultsDir.exists(), equalTo(true));
        File mergedIndexDir = new File(resultsDir, "part-00000/data/index");
        try (MMapDirectory mmap = new MMapDirectory(mergedIndexDir.toPath());) {
            SearcherFactory searcherFactory = new SearcherFactory();
            SearcherManager searcherManager = new SearcherManager(mmap, searcherFactory);
            TopDocs topDocs = searcherManager.acquire().search(new MatchAllDocsQuery(), 100);
            assertThat(topDocs.totalHits.value, equalTo((long) 100));
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(mmap);
            assertThat(segmentInfos.size(), equalTo(1));
        }
    }


    @Test
    public void testRetryMerge() throws Exception {
        // write data
        File inputDir = temporaryFolder.newFolder("input");
        writeInputFile(new File(inputDir, "part-r-00000.txt"), 0, 50);
        writeInputFile(new File(inputDir, "part-r-00001.txt"), 50, 50);

        // set up Solr configs
        File solrHomeDir = temporaryFolder.newFolder("solrHome");
        copySolrConfigs(solrHomeDir);

        // create an index
        File outputDir = new File(temporaryFolder.getRoot(), "output");

        final int numReducers = 10;
        ToolRunner.run(getConf(), new SolrIndexDriver(), new String[]{
                "-Dmapreduce.job.map.class=com.riskiq.solr.hadoop.IndexingTestCase$TestSolrMapper",
                "-Dmapreduce.job.partitioner.class=com.riskiq.solr.hadoop.SolrCloudCompositeIdRoutingPartitioner",
                "-Dsolr.record.writer.batch.size=20",
                "--solr-home-dir", "file://" + solrHomeDir.getAbsolutePath(),
                "--shards", "1",
                "--reducers", String.valueOf(numReducers),
                "--output-dir", outputDir.getAbsolutePath(),
                "-i", inputDir.getAbsolutePath()
        });

        File mergeConfigDir = temporaryFolder.newFolder("mergeConfig");
        File confDir = Files.createDirectory(new File(mergeConfigDir, "conf").toPath()).toFile();
        final String solrMergeConfigFile = "solrconfig_merge.xml";
        File mergeConfigFile = new File(confDir, solrMergeConfigFile);
        FileUtils.copyFile(new File(getClass().getResource(solrMergeConfigFile).toURI()), mergeConfigFile);

        // merge the index, but "fail" after the first iteration
        boolean interrupted = false;
        try {
            ToolRunner.run(getConf(), new SolrMergeDriver(), new String[]{
                    "-DmaxSegmentsOnTreeMerge=1",
                    "--shards", "1",
                    "--reducers", String.valueOf(numReducers),
                    "--input-dir", outputDir.getAbsolutePath(),
                    "--solr-home-dir", "file://" + mergeConfigDir.getAbsolutePath(),
                    "--solr-config-file-name", solrMergeConfigFile,
                    "--fanout", Integer.toString(numReducers / 2),
                    "--stop-after-iterations", "1"
            });
        } catch (InterruptedException e) {
            interrupted = true;
        }
        assertThat(interrupted, equalTo(true));

        // retry the merge
        int result = ToolRunner.run(getConf(), new SolrMergeDriver(), new String[]{
                "-DmaxSegmentsOnTreeMerge=1",
                "--shards", "1",
                "--reducers", String.valueOf(numReducers),
                "--input-dir", outputDir.getAbsolutePath(),
                "--solr-home-dir", "file://" + mergeConfigDir.getAbsolutePath(),
                "--solr-config-file-name", solrMergeConfigFile,
                "--fanout", Integer.toString(numReducers / 2)
        });
        assertThat(result, equalTo(0));

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
            assertThat(topDocs.totalHits.value, equalTo((long) 100));
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(mmap);
            assertThat(segmentInfos.size(), equalTo(1));
        }
    }
}