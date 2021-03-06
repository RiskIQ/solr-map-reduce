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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import com.riskiq.solr.hadoop.dedup.NoChangeUpdateConflictResolver;
import com.riskiq.solr.hadoop.dedup.RetainMostRecentUpdateConflictResolver;
import com.riskiq.solr.hadoop.dedup.UpdateConflictResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Iterator;

/**
 * This class loads the mapper's SolrInputDocuments into one EmbeddedSolrServer
 * per reducer. Each such reducer and Solr server can be seen as a (micro)
 * shard. The Solr servers store their data in HDFS.
 * 
 * More specifically, this class consumes a list of &lt;docId, SolrInputDocument&gt;
 * pairs, sorted by docId, and sends them to an embedded Solr server to generate
 * a Solr index shard from the documents.
 */
public class SolrReducer extends Reducer<Text, SolrInputDocumentWritable, Text, SolrInputDocumentWritable> {

  private UpdateConflictResolver resolver;
  private HeartBeater heartBeater;

  public static final String UPDATE_CONFLICT_RESOLVER = SolrReducer.class.getName() + ".updateConflictResolver";

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Class<? extends Throwable>[] RECOVERABLE_EXCEPTIONS = new Class[]{SolrServerException.class};
  
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    verifyPartitionAssignment(context);    
    SolrRecordWriter.addReducerContext(context);
    Class<? extends UpdateConflictResolver> resolverClass = context.getConfiguration().getClass(
        UPDATE_CONFLICT_RESOLVER, RetainMostRecentUpdateConflictResolver.class, UpdateConflictResolver.class);
    
    this.resolver = ReflectionUtils.newInstance(resolverClass, context.getConfiguration());
    /*
     * Note that ReflectionUtils.newInstance() above also implicitly calls
     * resolver.configure(context.getConfiguration()) if the resolver
     * implements org.apache.hadoop.conf.Configurable
     */
    
    this.heartBeater = new HeartBeater(context);
  }
  
  protected void reduce(Text key, Iterable<SolrInputDocumentWritable> values, Context context) throws IOException, InterruptedException {
    heartBeater.needHeartBeat();
    try {
      values = resolve(key, values, context);
      super.reduce(key, values, context);
    } catch (Exception e) {
      LOG.error("Unable to process key " + key, e);
      context.getCounter(getClass().getName() + ".errors", e.getClass().getName()).increment(1);
      if (isRecoverableException(e)) {
        LOG.warn("Ignoring recoverable exception: " + e.getMessage(), e);
      } else {
        throw new RuntimeException(e);
      }
    } finally {
      heartBeater.cancelHeartBeat();
    }
  }

  private Iterable<SolrInputDocumentWritable> resolve(
      final Text key, final Iterable<SolrInputDocumentWritable> values, final Context context) {
    
    if (resolver instanceof NoChangeUpdateConflictResolver) {
      return values; // fast path
    }
    return new Iterable<SolrInputDocumentWritable>() {
      @Override
      public Iterator<SolrInputDocumentWritable> iterator() {
        return new WrapIterator(resolver.orderUpdates(key, new UnwrapIterator(values.iterator()), context));
      }
    };
  }
    
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
    heartBeater.close();
    super.cleanup(context);
  }

  /*
   * Verify that if a mappers's partitioner sends an item to partition X it implies that said item
   * is sent to the reducer with taskID == X. This invariant is currently required for Solr
   * documents to end up in the right Solr shard.
   */
  private void verifyPartitionAssignment(Context context) {
    if ("true".equals(System.getProperty("verifyPartitionAssignment", "true"))) {
      String partitionStr = context.getConfiguration().get("mapred.task.partition");
      if (partitionStr == null) {
        partitionStr = context.getConfiguration().get("mapreduce.task.partition");
      }
      int partition = Integer.parseInt(partitionStr);
      int taskId = context.getTaskAttemptID().getTaskID().getId();
      Preconditions.checkArgument(partition == taskId, 
          "mapred.task.partition: " + partition + " not equal to reducer taskId: " + taskId);      
    }
  }


  /**
   * Determines whether or not the given {@link Throwable} constitutes an error from which the indexing process can
   * recover.
   * @param t an error which was thrown during the indexing process
   * @return true if we should log a warning and proceed; false if the given Throwable represents a fatal error
   */
  private boolean isRecoverableException(Throwable t) {
    while (true) {
      for (Class<?> clazz : RECOVERABLE_EXCEPTIONS) {
        if (clazz.isAssignableFrom(t.getClass())) {
          return true;
        }
      }
      Throwable cause = t.getCause();
      if (cause == null || cause == t) {
        return false;
      }
      t = cause;
    }

  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class WrapIterator implements Iterator<SolrInputDocumentWritable> {
    
    private Iterator<SolrInputDocument> parent;

    private WrapIterator(Iterator<SolrInputDocument> parent) {
      this.parent = parent;
    }

    @Override
    public boolean hasNext() {
      return parent.hasNext();
    }

    @Override
    public SolrInputDocumentWritable next() {
      return new SolrInputDocumentWritable(parent.next());
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }


  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class UnwrapIterator implements Iterator<SolrInputDocument> {
    
    private Iterator<SolrInputDocumentWritable> parent;

    private UnwrapIterator(Iterator<SolrInputDocumentWritable> parent) {
      this.parent = parent;
    }

    @Override
    public boolean hasNext() {
      return parent.hasNext();
    }

    @Override
    public SolrInputDocument next() {
      return parent.next().getSolrInputDocument();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }

}
