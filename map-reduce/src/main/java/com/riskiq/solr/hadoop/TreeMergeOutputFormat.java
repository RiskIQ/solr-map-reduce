/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.riskiq.solr.hadoop;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.misc.IndexMergeTool;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Version;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.index.MergePolicyFactory;
import org.apache.solr.index.MergePolicyFactoryArgs;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.store.hdfs.HdfsDirectory;
import org.apache.solr.store.hdfs.HdfsLockFactory;
import org.apache.solr.update.SolrIndexWriter;
import org.apache.solr.util.RTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * See {@link IndexMergeTool}.
 */
public class TreeMergeOutputFormat extends FileOutputFormat<Text, NullWritable> {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Configuration parameter which sets the read buffer size of {@link HdfsDirectory} in BYTES. Defaults to
   * {@link HdfsDirectory#DEFAULT_BUFFER_SIZE}, which, at the time of this writing, is 4096.
   */
  public static final String CONFIG_HDFS_DIRECTORY_BUFFER_SIZE = TreeMergeOutputFormat.class.getName() + ".hdfsDirectory.buffer.size";
  /**
   * Configuration parameter which sets the RAM buffer size of {@link IndexWriterConfig} in MEGABYTES. Defaults to
   * {@link IndexWriterConfig#DEFAULT_RAM_BUFFER_SIZE_MB} (16MB at the time of this writing).
   */
  public static final String CONFIG_INDEX_WRITER_RAM_BUFFER_SIZE = TreeMergeOutputFormat.class.getName() + ".indexWriter.buffer.size";

  public static final String CONFIG_SOLR_CONFIG_FILE_NAME = TreeMergeOutputFormat.class.getName() + ".solrConfig.fileName";
  public static final String DEFAULT_SOLR_CONFIG_FILE_NAME = "solrconfig.xml";


  public static void setHdfsDirectoryBufferSize(Job job, int sizeBytes) {
    job.getConfiguration().setInt(CONFIG_HDFS_DIRECTORY_BUFFER_SIZE, sizeBytes);
  }


  public static void setIndexWriterRamBufferSize(Job job, double sizeMB) {
    job.getConfiguration().setDouble(CONFIG_INDEX_WRITER_RAM_BUFFER_SIZE, sizeMB);
  }


  public static void setSolrConfigFileName(Job job, String path) {
    job.getConfiguration().set(CONFIG_SOLR_CONFIG_FILE_NAME, path);
  }


  @Override
  public RecordWriter getRecordWriter(TaskAttemptContext context) throws IOException {
    Utils.getLogConfigFile(context.getConfiguration());
    Path workDir = getDefaultWorkFile(context, "");
    return new TreeMergeRecordWriter(context, workDir);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class TreeMergeRecordWriter extends RecordWriter<Text,NullWritable> {
    
    private final Path workDir;
    private final List<Path> shards = new ArrayList();
    private final HeartBeater heartBeater;
    private final TaskAttemptContext context;
    
    private static final Logger LOG = log;

    public TreeMergeRecordWriter(TaskAttemptContext context, Path workDir) {
      this.workDir = new Path(workDir, "data/index");
      this.heartBeater = new HeartBeater(context);
      this.context = context;
    }
    
    @Override
    public void write(Text key, NullWritable value) {
      LOG.info("map key: {}", key);
      heartBeater.needHeartBeat();
      try {
        Path path = new Path(key.toString());
        shards.add(path);
      } finally {
        heartBeater.cancelHeartBeat();
      }
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException {
      LOG.debug("Task " + context.getTaskAttemptID() + " merging into dstDir: " + workDir + ", srcDirs: " + shards);
      writeShardNumberFile(context);      
      heartBeater.needHeartBeat();
      final int hdfsDirectoryBufferSize = context.getConfiguration().getInt(CONFIG_HDFS_DIRECTORY_BUFFER_SIZE, HdfsDirectory.DEFAULT_BUFFER_SIZE);
      try {
        Directory mergedIndex = new HdfsDirectory(workDir, HdfsLockFactory.INSTANCE, context.getConfiguration(), hdfsDirectoryBufferSize);

        Optional<MergePolicy> mergePolicyOptional = Optional.empty();
        String solrConfigFileName = context.getConfiguration().get(CONFIG_SOLR_CONFIG_FILE_NAME);
        if (!Strings.isNullOrEmpty(solrConfigFileName)) {
          // attempt to load a merge policy from the xml solr config
          Path solrHomeDir = SolrRecordWriter.findSolrConfig(context);
          if (!solrHomeDir.toString().contains(":/")) {
            solrHomeDir = new Path("file://" + solrHomeDir.toString());
          }
          log.info("Using Solr home directory {}", solrHomeDir);
          try {
            mergePolicyOptional = buildMergePolicy(new File(solrHomeDir.toUri()).toPath(), solrConfigFileName);
          } catch (SAXException | ParserConfigurationException e) {
            throw new IOException("Unable to parse Solr configuration from " + solrHomeDir, e);
          }
        }

        // TODO: shouldn't we pull the Version from the solrconfig.xml?
        IndexWriterConfig writerConfig = new IndexWriterConfig(null)
                .setOpenMode(OpenMode.CREATE)
                .setUseCompoundFile(false)
                .setRAMBufferSizeMB(context.getConfiguration().getDouble(CONFIG_INDEX_WRITER_RAM_BUFFER_SIZE, IndexWriterConfig.DEFAULT_RAM_BUFFER_SIZE_MB))
            //.setMergePolicy(mergePolicy) // TODO: grab tuned MergePolicy from solrconfig.xml?
            //.setMergeScheduler(...) // TODO: grab tuned MergeScheduler from solrconfig.xml?
            ;
        mergePolicyOptional.ifPresent(writerConfig::setMergePolicy);

        if (LOG.isDebugEnabled()) {
          writerConfig.setInfoStream(System.out);
        }
//        writerConfig.setMaxThreadStates(1);
        
        // disable compound file to improve performance
        // also see http://lucene.472066.n3.nabble.com/Questions-on-compound-file-format-td489105.html
        // also see defaults in SolrIndexConfig
        MergePolicy mergePolicy = writerConfig.getMergePolicy();
        LOG.debug("mergePolicy was: {}", mergePolicy);
        if (mergePolicy instanceof TieredMergePolicy) {
          ((TieredMergePolicy) mergePolicy).setNoCFSRatio(0.0);
//          ((TieredMergePolicy) mergePolicy).setMaxMergeAtOnceExplicit(10000);          
//          ((TieredMergePolicy) mergePolicy).setMaxMergeAtOnce(10000);       
//          ((TieredMergePolicy) mergePolicy).setSegmentsPerTier(10000);
        } else if (mergePolicy instanceof LogMergePolicy) {
          mergePolicy.setNoCFSRatio(0.0);
        }
        LOG.info("Using mergePolicy: {}", mergePolicy);
        
        IndexWriter writer = new IndexWriter(mergedIndex, writerConfig);
        
        Directory[] indexes = new Directory[shards.size()];
        for (int i = 0; i < shards.size(); i++) {
          indexes[i] = new HdfsDirectory(shards.get(i), HdfsLockFactory.INSTANCE, context.getConfiguration(), hdfsDirectoryBufferSize);
        }

        context.setStatus("Logically merging " + shards.size() + " shards into one shard");
        LOG.info("Logically merging " + shards.size() + " shards into one shard: " + workDir);
        RTimer timer = new RTimer();
        
        writer.addIndexes(indexes); 
        // TODO: avoid intermediate copying of files into dst directory; rename the files into the dir instead (cp -> rename) 
        // This can improve performance and turns this phase into a true "logical" merge, completing in constant time.
        // See https://issues.apache.org/jira/browse/LUCENE-4746

        timer.stop();
        if (LOG.isDebugEnabled()) {
          context.getCounter(SolrCounters.class.getName(), SolrCounters.LOGICAL_TREE_MERGE_TIME.toString()).increment((long) timer.getTime());
        }
        LOG.info("Logical merge took {}ms", timer.getTime());
        int maxSegments = context.getConfiguration().getInt(TreeMergeMapper.MAX_SEGMENTS_ON_TREE_MERGE, Integer.MAX_VALUE);
        context.setStatus("Optimizing Solr: forcing mtree merge down to " + maxSegments + " segments");
        LOG.info("Optimizing Solr: forcing tree merge down to {} segments", maxSegments);
        timer = new RTimer();
        if (maxSegments < Integer.MAX_VALUE) {
          writer.forceMerge(maxSegments);
          // TODO: consider perf enhancement for no-deletes merges: bulk-copy the postings data 
          // see http://lucene.472066.n3.nabble.com/Experience-with-large-merge-factors-tp1637832p1647046.html
        }
        timer.stop();
        if (LOG.isDebugEnabled()) {
          context.getCounter(SolrCounters.class.getName(), SolrCounters.PHYSICAL_TREE_MERGE_TIME.toString()).increment((long) timer.getTime());
        }
        LOG.info("Optimizing Solr: done forcing tree merge down to {} segments in {}ms", maxSegments, timer.getTime());

        // Set Solr's commit data so the created index is usable by SolrCloud. E.g. Currently SolrCloud relies on
        // commitTimeMSec in the commit data to do replication.
        SolrIndexWriter.setCommitData(writer, 1);

        timer = new RTimer();
        LOG.info("Optimizing Solr: Closing index writer");
        writer.close();
        LOG.info("Optimizing Solr: Done closing index writer in {}ms", timer.getTime());
        context.setStatus("Done");
      } finally {
        heartBeater.cancelHeartBeat();
        heartBeater.close();
      }
    }


    /**
     * Attempts to read the Solr configuration file with the given name in the given path and construct a {@link MergePolicy}
     * using the values found therein.
     * @param solrHomePath a path containing a Solr config xml file
     * @param configName name of the Solr config xml file, including the ".xml" suffix
     * @return an {@link Optional} containing a MergePolicy if we were able to successfully construct a policy using the
     * loaded config file; an empty Optional if we were unable to do so
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     */
    private Optional<MergePolicy> buildMergePolicy(java.nio.file.Path solrHomePath, String configName) throws IOException, SAXException, ParserConfigurationException {
      SolrConfig solrConfig = new SolrConfig(solrHomePath, "conf/" + configName, null,true);
      PluginInfo mergePolicyFactoryInfo = solrConfig.indexConfig.mergePolicyFactoryInfo;
      if (mergePolicyFactoryInfo == null) {
        return Optional.empty();
      } else {
        IndexSchema schema = new IndexSchema("schema", new InputSource(new BufferedReader(new FileReader(new File(solrHomePath.toFile(), "conf/schema.xml")))), Version.LATEST, new SolrResourceLoader());
        String className = mergePolicyFactoryInfo.className;
        MergePolicyFactoryArgs args = new MergePolicyFactoryArgs(mergePolicyFactoryInfo.initArgs);
        MergePolicyFactory mergePolicyFactory = solrConfig.getResourceLoader().newInstance(className, MergePolicyFactory.class, new String[0], new Class[]{SolrResourceLoader.class, MergePolicyFactoryArgs.class, IndexSchema.class}, new Object[]{solrConfig.getResourceLoader(), args, schema});
        return Optional.of(mergePolicyFactory.getMergePolicy());
      }
    }


    /*
     * For background see MapReduceIndexerTool.renameTreeMergeShardDirs()
     * 
     * Also see MapReduceIndexerTool.run() method where it uses
     * NLineInputFormat.setNumLinesPerSplit(job, options.fanout)
     */
    private void writeShardNumberFile(TaskAttemptContext context) throws IOException {
      Preconditions.checkArgument(shards.size() > 0);
      String shard = shards.get(0).getParent().getParent().getName(); // move up from "data/index"
      String taskId = shard.substring("part-m-".length(), shard.length()); // e.g. part-m-00001
      int taskNum = Integer.parseInt(taskId);
      int outputShardNum = taskNum / shards.size();
      LOG.debug("Merging into outputShardNum: " + outputShardNum + " from taskId: " + taskId);
      Path shardNumberFile = new Path(workDir.getParent().getParent(), TreeMergeMapper.SOLR_SHARD_NUMBER);
      OutputStream out = shardNumberFile.getFileSystem(context.getConfiguration()).create(shardNumberFile);
      Writer writer = new OutputStreamWriter(out, StandardCharsets.UTF_8);
      writer.write(String.valueOf(outputShardNum));
      writer.flush();
      writer.close();
    }    
  }
}
