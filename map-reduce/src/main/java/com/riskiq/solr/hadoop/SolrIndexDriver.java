package com.riskiq.solr.hadoop;

import com.riskiq.mapreduce.io.FilenameInputFormat;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * MapReduce driver which will facilitates offline creation of Solr indexes.
 * Note that there is no default mapper class for this driver, so the {@code mapreduce.job.map.class} configuration
 * parameter should be used to configure a mapper.
 * This driver is intended to be used in conjunction with {@link SolrMergeDriver}.
 * @author Joe Linn
 * 12/12/2019
 */
public class SolrIndexDriver extends Configured implements Tool {
    private static final Logger log = LoggerFactory.getLogger(SolrIndexDriver.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SolrIndexDriver(), args);
        System.exit(res);
    }


    @Override
    public int run(String[] args) throws Exception {
        Options options = new Options();

        Option solrHomeOption = new Option("shd", "solr-home-dir", true, "Absolute path containing Solr conf/ dir");
        solrHomeOption.setRequired(true);
        options.addOption(solrHomeOption);

        Option shardsOption = new Option("s", "shards", true, "Number of shards.");
        shardsOption.setRequired(true);
        options.addOption(shardsOption);

        Option reducersOption = new Option("r", "reducers", true, "Number of reducers.");
        reducersOption.setRequired(true);
        options.addOption(reducersOption);

        Option outputOption = new Option("o", "output-dir", true, "Output directory.");
        outputOption.setRequired(true);
        options.addOption(outputOption);

        Option inputOption = new Option("i", "input-dir", true, "Input directory.");
        inputOption.setRequired(true);
        options.addOption(inputOption);

        Option log4jOption = new Option("l4", "log4j", true, "log4j file location");
        options.addOption(log4jOption);

        Option overwriteOption = new Option("ow", "overwrite", false, "If true, any existing data in the output directory will be deleted.");
        options.addOption(overwriteOption);

        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);

        String input = commandLine.getOptionValue(inputOption.getOpt());

        Job job = Job.getInstance(getConf());
        job.setJobName("Solr offline index creator: " + input);
        job.setJarByClass(getClass());

        if (commandLine.hasOption(log4jOption.getOpt())) {
            Utils.configureLog4jProperties(commandLine.getOptionValue(log4jOption.getOpt()));
        }

        // copy Solr config files to a temporary directory
        File solrHomeDir = new File(new Path(commandLine.getOptionValue(solrHomeOption.getOpt())).toUri());
        File tmpSolrHomeDir = Utils.copySolrConfigToTempDir(solrHomeDir, "core1");

        SolrOutputFormat.setupSolrHomeCache(tmpSolrHomeDir, job);

        job.setOutputFormatClass(SolrOutputFormat.class);

        job.getConfiguration().setInt(SolrCloudPartitioner.SHARDS, Integer.parseInt(commandLine.getOptionValue(shardsOption.getOpt())));

        job.setNumReduceTasks(Integer.parseInt(commandLine.getOptionValue(reducersOption.getOpt())));

        // set up input
        FilenameInputFormat.addInputPaths(job, input);
        job.setInputFormatClass(FilenameInputFormat.class);

        // set up output
        Path outputReduceDir = new Path(commandLine.getOptionValue(outputOption.getOpt()), "reducers");
        if (commandLine.hasOption(overwriteOption.getOpt())) {
            // delete output directory
            Path outputPath = new Path(commandLine.getOptionValue(outputOption.getOpt()));
            if (outputPath.getFileSystem(getConf()).exists(outputPath)) {
                outputPath.getFileSystem(getConf()).delete(outputPath, true);
            }
        }
        FileOutputFormat.setOutputPath(job, outputReduceDir);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SolrInputDocumentWritable.class);

        if (job.getConfiguration().get(JobContext.REDUCE_CLASS_ATTR) == null) { // enable customization
            job.setReducerClass(SolrReducer.class);
        }

        log.info("Starting MR job with input {} and output {}", input, outputReduceDir);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
