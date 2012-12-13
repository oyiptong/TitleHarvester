package org.mozilla.up;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.util.Tool;


/**
 * Created with IntelliJ IDEA.
 * User: oyiptong
 * Date: 2012-12-06
 * Time: 2:35 AM
 */
public class TitleHarvester extends Configured implements Tool
{
    public static class CSVOutputFormat extends TextOutputFormat<Text, Text>
    {
        public RecordWriter<Text, Text> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException
        {
            Path file = FileOutputFormat.getTaskOutputPath(job, name);
            FileSystem fs = file.getFileSystem(job);
            FSDataOutputStream fileOut = fs.create(file, progress);
            return new CSVRecordWriter(fileOut);
        }

        protected static class CSVRecordWriter implements RecordWriter<Text, Text>
        {
            protected DataOutputStream outStream;

            public CSVRecordWriter(DataOutputStream out)
            {
                this.outStream = out;
            }

            public synchronized void write(Text key, Text value) throws IOException
            {
                CsvRecordOutput csvOutput = new CsvRecordOutput(outStream);
                csvOutput.writeString(key.toString(), "url");
                csvOutput.writeString(value.toString(), "title");
            }

            public synchronized void close(Reporter reporter) throws IOException
            {
                outStream.close();
            }
        }
    }

    public static class TitleHarvestMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text>
    {
        // represent ruleset for blekko data, including regular expression

        public void map(Text url, Text metadataText, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException
        {
           // output {url: category} and {url: title}
            JsonFactory f = new JsonFactory();
            JsonParser p = f.createJsonParser(metadataText.toString());

            // get rid of START_OBJECT
            p.nextToken();
            boolean htmlDoc = false;
            String title = null;
            int statusCode = -1;

            while (p.nextToken() != JsonToken.END_OBJECT)
            {
                String namefield = p.getCurrentName();
                JsonToken token = p.nextToken(); // move to value

                if (namefield != null && namefield.equals("http_result"))
                {
                    statusCode = p.getIntValue();
                    if (statusCode < 200 || statusCode >= 300)
                    {
                        break;
                    }
                }
                else if (token == JsonToken.START_OBJECT)
                {
                    String objName = p.getCurrentName();

                    while (p.nextToken() != JsonToken.END_OBJECT)
                    {
                        String subObjName = p.getCurrentName();
                        if (p.getCurrentToken() == JsonToken.START_ARRAY) {
                            // processing links or meta_tags
                            while (p.nextToken() != JsonToken.END_ARRAY) {
                                // do nothing
                            }
                        }
                        else if (subObjName != null &&  namefield.equals("content"))
                        {
                            if (subObjName.equals("title"))
                            {
                                p.nextToken();
                                title = p.getText();
                            }
                            else if (subObjName.equals("type"))
                            {
                                p.nextToken();
                                if (p.getText().equals("html-doc"))
                                {
                                    htmlDoc = true;
                                }
                            }

                        }
                    }
                }
                if (statusCode > -1 && title != null && htmlDoc)
                {
                    collector.collect(url, new Text("title:"+title));
                    break;
                }
            }
        }
    }

    public static class TitleHarvestReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text url, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException
        {
            // write the two key/value pairs
           while (values.hasNext())
           {
               String value = values.next().toString();

               int sepIndex = value.indexOf(":");
               String type = value.substring(0, sepIndex);
               String data = value.substring(sepIndex+1);

               collector.collect(url, new Text(data));
           }
        }
    }

    public static void main(String[] args) throws Exception
    {
        ToolRunner.run(new TitleHarvester(), args);
    }

    public int run(String[] args) throws Exception
    {
        // Creates a new job configuration for this Hadoop job.

        JobConf conf = new JobConf(getConf(), getClass());
        conf.setJobName(getClass().getName());

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(TitleHarvestMapper.class);
        conf.setReducerClass(TitleHarvestReducer.class);
        conf.setNumReduceTasks(10);

        conf.setInputFormat(SequenceFileInputFormat.class);
        FileSystem fs = FileSystem.get(conf);

        // read from list of valid segment files
        String segmentList = args[0];
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path(segmentList))));
        String line;
        line = br.readLine();
        while (line != null)
        {
            String segmentPath = String.format("/aws-publicdatasets/common-crawl/parse-output/segment/%1$s/metadata-*", line);
            System.out.println(String.format("Adding file at: %1$s", segmentPath));

            FileInputFormat.addInputPath(conf, new Path(segmentPath));
            line = br.readLine();
        }

        /*
        CSVOutputFormat.setOutputPath(conf, new Path(args[1]));
        CSVOutputFormat.setCompressOutput(conf, false);
        conf.setOutputFormat(CSVOutputFormat.class);
        */

        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        // Allows some (50%) of tasks fail; we might encounter the
        // occasional troublesome set of records and skipping a few
        // of 1000s won't hurt counts too much.
        conf.set("mapred.max.map.failures.percent", "50");


        // Tells Hadoop mappers and reducers to pull dependent libraries from
        // those bundled into this JAR.
        conf.setJarByClass(TitleHarvester.class);

        // Runs the job.
        JobClient.runJob(conf);

        return 0;
    }
}
