package com.riskiq.solr.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;

import java.io.IOException;
import java.net.URL;

/**
 * @author ahunt
 */
public class MapReduceLocalRunnerTest {

    private Configuration conf;

    @Before
    public void setup() throws Exception {
        Logger.getRootLogger().setLevel(Level.DEBUG);
        Logger.getLogger("org.apache.parquet.hadoop.ColumnChunkPageWriteStore").setLevel(Level.WARN);

        System.setProperty("hadoop.home.dir", "/");

        conf = new Configuration();

        conf.set("mapreduce.jobtracker.address", "local");
        conf.set("fs.defaultFS", "file:///");
    }

    protected void setupInput(Path inputPath, URL ... inputFiles) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(inputPath)) {
            fs.delete(inputPath, true);
        }
        fs.mkdirs(inputPath);

        for (URL inputFile: inputFiles) {
            Path resource = new Path(inputFile.getPath());
            fs.copyFromLocalFile(resource, inputPath);
        }
    }

    protected void setupOutput(Path outputPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
    }

    protected Configuration getConf() {
        return conf;
    }

    protected boolean runJob(Job job, Path inputPath, Path outputPath)
            throws ClassNotFoundException, IOException, InterruptedException {
        Logger.getRootLogger().setLevel(Level.DEBUG);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true);
    }
}
