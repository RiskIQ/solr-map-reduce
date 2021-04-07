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

import com.google.common.annotations.Beta;
import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.common.util.SuppressForbidden;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;


@Beta
public final class Utils {
  private static final Logger log = LoggerFactory.getLogger(Utils.class);
  
  private static final String LOG_CONFIG_FILE = "hadoop.log4j.configuration";
  
  public static void setLogConfigFile(File file, Configuration conf) {
    conf.set(LOG_CONFIG_FILE, file.getName());
  }

  public static void getLogConfigFile(Configuration conf) {
    String log4jPropertiesFile = conf.get(LOG_CONFIG_FILE);
    configureLog4jProperties(log4jPropertiesFile);
  }

  @SuppressForbidden(reason = "method is specific to log4j")
  public static void configureLog4jProperties(String log4jPropertiesFile) {
    if (log4jPropertiesFile != null) {
      PropertyConfigurator.configure(log4jPropertiesFile);
    }
  }

  public static String getShortClassName(Class clazz) {
    return getShortClassName(clazz.getName());
  }
  
  public static String getShortClassName(String className) {
    int i = className.lastIndexOf('.'); // regular class
    int j = className.lastIndexOf('$'); // inner class
    return className.substring(1 + Math.max(i, j));
  }


  /**
   * Copies Solr config files in the given home directory to a temporary directory for use in distributed cache
   * @param solrHomeDir directory which contains a "conf" subdirectory. "conf" should contain Solr config xml files.
   * @param coreName name of the Solr core. Typically "core1" for mapreduce purposes.
   * @return the temporary directory to which the given configs were copied
   * @throws IOException
   */
  public static File copySolrConfigToTempDir(File solrHomeDir, String coreName) throws IOException {
    File tmpSolrHomeDir = Files.createTempDirectory("solr-home-").toFile();
    File tmpCoreDir = new File(tmpSolrHomeDir, coreName);
    Files.createDirectory(tmpCoreDir.toPath());
    File solrConfDir = new File(solrHomeDir, "conf");
    if (!solrConfDir.exists() || !solrConfDir.isDirectory()) {
      throw new IllegalStateException("Solr conf directory " + solrConfDir.getAbsolutePath() + " not found.");
    }
    FileUtils.copyDirectory(solrHomeDir, tmpSolrHomeDir);
    // copy config files to <solrHomeDir>/<coreName>.  Those files will be used in the reduce phase.
    FileUtils.copyDirectory(solrConfDir, tmpCoreDir);
    return tmpSolrHomeDir;
  }


  /**
   * Deletes the Solr home zip file which is created for use in distributed cache.
   * @param job the job that used the zip file
   */
  public static void cleanUpSolrHomeCache(JobContext job) {
    String pathString = job.getConfiguration().get(SolrOutputFormat.SETUP_OK);
    if (Strings.isNullOrEmpty(pathString)) {
      // nothing to clean up
      return;
    }
    Path zipPath = new Path(pathString);
    try {
      zipPath.getFileSystem(job.getConfiguration())
              .delete(zipPath, false);
    } catch (IOException e) {
      log.error("Unable to delete Solr home zip file at " + pathString, e);
    }
  }
}
