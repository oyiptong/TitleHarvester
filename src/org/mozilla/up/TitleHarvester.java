package org.mozilla.up;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import com.google.common.net.InternetDomainName;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.util.Tool;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

/**
 * Created with IntelliJ IDEA.
 * User: oyiptong
 * Date: 2012-12-06
 * Time: 2:35 AM
 */
public class TitleHarvester extends Configured implements Tool
{

    public static class TitleHarvestMapper extends Mapper<Text, Text, Text, Text>
    {
        // represent ruleset for blekko data, including regular expression

        private static Map<String, ArrayList<String>> domainCategories;
        private static Joiner joiner = Joiner.on(',').skipNulls();
        private static Pattern wwwPattern = Pattern.compile("www\\w*");

        private void setupCategories()
        {
            try
            {
                ObjectMapper mapper = new ObjectMapper();
                domainCategories = mapper.readValue(getClass().getResourceAsStream("/data/domain_cat_index.json"), new TypeReference<Map<String, ArrayList<String>>>(){});
            } catch(IOException e)
            {
                //TODO: log this?
            }
        }

        @Override
        protected void setup(Context context) throws InterruptedException, IOException {
            super.setup(context);
            setupCategories();
        }

        private String getTitle(String metadata) throws IOException
        {
            /* parse json metadata and obtain title */

            JsonFactory f = new JsonFactory();
            JsonParser p = f.createJsonParser(metadata);

            // get rid of START_OBJECT
            p.nextToken();
            boolean htmlDoc = false;
            String title = null;
            int statusCode = -1;

            while (p.nextToken() != JsonToken.END_OBJECT)
            {
                String nameField = p.getCurrentName();
                JsonToken token = p.nextToken(); // move to value

                if (nameField != null && nameField.equals("http_result"))
                {
                    statusCode = p.getIntValue();
                    if (statusCode < 200 || statusCode >= 300)
                    {
                        return null;
                    }
                } else if (token == JsonToken.START_OBJECT)
                {
                    while (p.nextToken() != JsonToken.END_OBJECT)
                    {
                        String subObjName = p.getCurrentName();
                        if (p.getCurrentToken() == JsonToken.START_ARRAY) {
                            // processing links or meta_tags
                            while (p.nextToken() != JsonToken.END_ARRAY) {
                                // do nothing
                            }
                        } else if (subObjName != null &&  nameField.equals("content"))
                        {
                            if (subObjName.equals("title"))
                            {
                                p.nextToken();
                                title = p.getText();
                            } else if (subObjName.equals("type"))
                            {
                                p.nextToken();
                                if (p.getText().equals("html-doc"))
                                {
                                    htmlDoc = true;
                                } else
                                {
                                    return null;
                                }
                            }

                        }
                    }
                }
                if (statusCode > -1 && title != null && htmlDoc)
                {
                    return title.replaceAll("\\s+", " ").trim();
                }
            }
            return null;
        }

        private ArrayList<String> getCategories(String url)
        {
            /*
                Obtain categories for the given url.

                This version only checks if the hostname is found in the domain map.
                Later versions will also look for path prefixes and wildcards
             */
            try
            {
                URL uri = new URL(url.toLowerCase());
                String hostName = uri.getHost();
                InternetDomainName domainName = InternetDomainName.from(uri.getHost());
                String domainStr = domainName.topPrivateDomain().name();

                if (!hostName.equals(domainStr))
                {
                    int domainIndex = hostName.indexOf(domainStr);
                    try
                    {
                        String subDomains = hostName.substring(0, domainIndex-1);
                        if (wwwPattern.matcher(subDomains).matches())
                        {
                            hostName = domainStr;
                        }
                    } catch (StringIndexOutOfBoundsException e)
                    {
                        // for when the hostName is longer than the domainStr
                        System.out.println(String.format("StringIndexOutOfBoundsException hostname:%1$s domainStr:%2$s %3$s", hostName, domainStr, url));
                    }
                }

                if (domainCategories.containsKey(hostName))
                {
                    return domainCategories.get(hostName);
                } else
                {
                    //TODO: log this and/or collect stats
                }

            } catch (MalformedURLException e)
            {
                //TODO: log this and/or collect stats
                System.out.println("URISyntaxException " + url);
                return null;
            } catch (IllegalArgumentException e)
            {
                //TODO: in case this is an IP address. log and collect stats
                System.out.println("IllegalArgumentException " + url);
                return null;
            } catch (NullPointerException e)
            {
                //Error reading uri.getHost()
                System.out.println("NullPointerException " + url);
                return null;
            } catch(IllegalStateException e)
            {
                // Not under public suffix
                System.out.println("IllegalStateException " + url);
                return null;
            }
            return null;
        }

        @Override
        public void map(Text url, Text metadataText, Context context) throws InterruptedException, IOException
        {
           // output {url: category} and {url: title}

            /*
                Find if metadata should be parsed, based on the url.
                The url should:
                    a) be found in the list
                    b) have at least one category classification
             */

            ArrayList<String> categories = getCategories(url.toString());

            if (categories != null) {
                String title = getTitle(metadataText.toString());
                if (title != null)
                {
                    context.write(url, new Text("title:" + title));
                    context.write(url, new Text("categories:" + joiner.join(categories)));
                }
            }
        }
    }

    public static class TitleHarvestReducer extends Reducer<Text, Text, Text, Text>
    {
        private static Splitter splitter = Splitter.on(':').trimResults().limit(2);

        @Override
        public void reduce(Text url, Iterable<Text> values, Context context) throws InterruptedException, IOException
        {
            String title = null;
            String categories = null;

            for (Text value : values)
            {
                String valueStr = value.toString();

                ArrayList<String> splitValues = Lists.newArrayList(splitter.split(valueStr));
                String dataType = splitValues.get(0);
                String data = splitValues.get(1);

                if (dataType.equals("title"))
                {
                    title = data;
                } else if (dataType.equals("categories"))
                {
                    categories = data;
                }
            }
            if (title != null && categories != null)
            {
                context.write(url, new Text(String.format("%1$s\t%2$s", title, categories)));
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new TitleHarvester(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception
    {
        // Creates a new job configuration for this Hadoop job.
        if (args.length < 2)
        {

            System.err.printf("Usage: %s [generic options] <segment_file_path> <output_path>\n", getClass().getSimpleName());

            ToolRunner.printGenericCommandUsage(System.err);

            return -1;

        }

        Job job = new Job(getConf());

        // Tells Hadoop mappers and reducers to pull dependent libraries from
        // those bundled into this JAR.
        job.setJarByClass(getClass());

        job.setJobName(getClass().getName());

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(TitleHarvestMapper.class);
        job.setReducerClass(TitleHarvestReducer.class);
        //job.setNumReduceTasks(10);

        job.setInputFormatClass(SequenceFileInputFormat.class);

        // read from list of valid segment files
        String segmentList = args[0];

        // if segment list url is on s3, assume input is on s3
        String pathPrefix = "";
        if (segmentList.startsWith("s3n://"))
        {
            pathPrefix = "s3n:/";
        }

        Path path = new Path(segmentList);
        FileSystem fs = path.getFileSystem(job.getConfiguration()); // get FS appropriate for input type
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line = br.readLine();

        while (line != null)
        {
            String segmentPath = String.format("%1$s/aws-publicdatasets/common-crawl/parse-output/segment/%2$s/metadata-*", pathPrefix, line);
            System.out.println(String.format("Adding file at: %1$s", segmentPath));

            FileInputFormat.addInputPath(job, new Path(segmentPath));
            line = br.readLine();
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Allows some (50%) of tasks fail; we might encounter the
        // occasional troublesome set of records and skipping a few
        // of 1000s won't hurt counts too much.
        job.getConfiguration().set("mapred.max.map.failures.percent", "50");

        // Runs the job.
        return job.waitForCompletion(true) ? 0 : 1;
    }
}