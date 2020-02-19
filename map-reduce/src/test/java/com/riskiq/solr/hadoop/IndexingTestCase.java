package com.riskiq.solr.hadoop;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Joe Linn
 * 01/10/2020
 */
public abstract class IndexingTestCase extends MapReduceLocalRunnerTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();


    protected void writeInputFile(File file, int startID, int numLines) throws IOException {
        List<String> lines = new ArrayList<>(numLines);
        for (int i = startID; i < startID + numLines; i++) {
            lines.add(i + "\tvalue" + i);
        }
        FileUtils.writeLines(file, lines);
    }


    protected void copySolrConfigs(File solrHomeDir) throws IOException, URISyntaxException {
        File confDir = Files.createDirectory(new File(solrHomeDir, "conf").toPath()).toFile();
        FileUtils.copyFile(new File(getClass().getResource("solrconfig.xml").toURI()), new File(confDir, "solrconfig.xml"));
        FileUtils.copyFile(new File(getClass().getResource("schema.xml").toURI()), new File(confDir, "schema.xml"));
    }


    public static class TestSolrMapper extends SolrMapper<IntWritable, Text> {
        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            Path filePath = new Path(value.toString());
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(context.getConfiguration()).open(filePath)))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\t");
                    SolrInputDocument doc = new SolrInputDocument();
                    doc.setField("id", parts[0]);
                    doc.setField("value", parts[1]);
                    context.write(new Text(parts[0]), new SolrInputDocumentWritable(doc));
                }
            }
        }
    }
}
