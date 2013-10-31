package org.mozilla.up;

import com.google.common.base.Joiner;
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
    enum ParseStats
    {
        ERR_SETUP,
        ERR_URI_MALFORMED,
        ERR_URI_ILLEGAL,
        ERR_URI_PARSE,
        ERR_URI_NO_SUFFIX,
        SUCCESS,
        PAGE_NO_CATEGORY,
        PAGE_NO_TITLE,
        PAGE_NO_KEYWORDS,
        DATA_NO_JSON,
    }

    enum PageHTTPStatus
    {
        INFO_1XX,
        SUCCESS_2XX,
        REDIRECTION_3XX,
        ERROR_4XX,
        ERROR_5XX,
        UNDEFINED,
    }

    enum CategoryStats
    {
        ONE,
        TWO,
        THREE,
        FOUR,
        FIVE,
        MORE,
    }

    enum URLOccurrence
    {
        ONE,
        TWO,
        THREE,
        FOUR,
        FIVE,
        MORE,
    }

    public static final String DEFAULT_REGION_CODE = "en-US";

    public static class TitleHarvestMapper extends Mapper<Text, Text, Text, Text>
    {
        // represent ruleset for blekko data, including regular expression

        private static Map<String, ArrayList<String>> domainCategories;
        private static Joiner joiner = Joiner.on(',').skipNulls();
        private static Pattern wwwPattern = Pattern.compile("www\\w*");

        private void setupCategories(Context context) throws IOException
        {
            try
            {
                ObjectMapper mapper = new ObjectMapper();
                String filePath = null;
                String regionCode = context.getConfiguration().get("titleHarvester.regionCode");
                String variation = context.getConfiguration().get("titleHarvester.variation");

                if (null == variation || variation.isEmpty()) {
                	filePath = String.format("/data/domain_cat_index/%1$s.json", regionCode);
                } else {
                	filePath = String.format("/data/domain_cat_index/%1$s.%2$s.json", regionCode, variation);
                }
                domainCategories = mapper.readValue(getClass().getResourceAsStream(filePath), new TypeReference<Map<String, ArrayList<String>>>(){});
            } catch(IOException e)
            {
                context.getCounter(ParseStats.ERR_SETUP).increment(1);
                throw e;
            }
        }

        @Override
        protected void setup(Context context) throws InterruptedException, IOException {
            super.setup(context);
            setupCategories(context);
        }

        private void logStatusCode(Context context, int statusCode)
        {
            // Log non-200 status codes
            if (statusCode < 200)
            {
                context.getCounter(PageHTTPStatus.INFO_1XX).increment(1);

            } else if (statusCode < 300)
            {
                context.getCounter(PageHTTPStatus.SUCCESS_2XX).increment(1);

            } else if (statusCode < 400)
            {
                context.getCounter(PageHTTPStatus.REDIRECTION_3XX).increment(1);

            } else if (statusCode < 500)
            {
                context.getCounter(PageHTTPStatus.ERROR_4XX).increment(1);

            } else
            {
                context.getCounter(PageHTTPStatus.ERROR_5XX).increment(1);
            }
        }

        /**
         * Get the title and the keywords in meta tags.
         * @param metadata
         * @param context
         * @return
         * @throws IOException
         */
        private String[] getTitleKeywords(String metadata, Context context) throws IOException
        {
            /* parse json metadata and obtain title */

            if (metadata == null || metadata.trim().isEmpty())
            {
                context.getCounter(ParseStats.DATA_NO_JSON).increment(1);
                return null;
            }

            String[] titleKeywords = {null, null};

            JsonFactory f = new JsonFactory();
            JsonParser p = f.createJsonParser(metadata);

            // get rid of START_OBJECT
            p.nextToken();
            boolean htmlDoc = false;
            int statusCode = -1;

            while (p.nextToken() != JsonToken.END_OBJECT)
            {
                String nameField = p.getCurrentName();
                JsonToken token = p.nextToken(); // move to value

                if (nameField != null && nameField.equals("http_result"))
                {
                    statusCode = p.getIntValue();
                    logStatusCode(context, statusCode);

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
                                // get keywords
                                if ("name".equals(p.getCurrentName())) {
                                    p.nextToken();
                                    if ("keywords".equals(p.getText())) {
                                        p.nextToken();
                                        if ("content".equals(p.getCurrentName())) {
                                            p.nextToken();
                                            titleKeywords[1] = p.getText().replaceAll("\\s+", " ").trim();
                                        }
                                    }
                                }
                            }

                        } else if (subObjName != null &&  nameField.equals("content"))
                        {
                            if (subObjName.equals("title"))
                            {
                                p.nextToken();
                                titleKeywords[0] = p.getText().replaceAll("\\s+", " ").trim();

                            } else if (subObjName.equals("type"))
                            {
                                p.nextToken();
                                String pageType = p.getText();
                                context.getCounter("DATA_PAGE_TYPE", pageType).increment(1);
                                if (pageType.equals("html-doc"))
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
            }

            if (statusCode == -1)
            {
                context.getCounter(PageHTTPStatus.UNDEFINED).increment(1);
            } else if (!htmlDoc)
            {
                context.getCounter("DATA_PAGE_TYPE", "undefined").increment(1);
            } else if (titleKeywords[0] == null || titleKeywords[0].isEmpty())
            {
                System.out.println("!!!Counter: " + context.getCounter(ParseStats.PAGE_NO_TITLE));
                context.getCounter(ParseStats.PAGE_NO_TITLE).increment(1);
            } else {
                if (titleKeywords[1] == null || titleKeywords[1].isEmpty()) {
                    context.getCounter(ParseStats.PAGE_NO_KEYWORDS).increment(1);
                }
                return titleKeywords;
            }

            return null;
        }

        private ArrayList<String> getCategories(String url, Context context)
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

                    if (domainIndex > 1)
                    {
                        try
                        {
                            String subDomains = hostName.substring(0, domainIndex-1);
                            if (wwwPattern.matcher(subDomains).matches())
                            {
                                hostName = domainStr;
                            }
                        } catch (StringIndexOutOfBoundsException e)
                        {
                            // erroneous url
                            context.getCounter(ParseStats.ERR_URI_MALFORMED).increment(1);
                            return null;
                        }
                    } else
                    {
                        // url probably wrong
                        context.getCounter(ParseStats.ERR_URI_MALFORMED).increment(1);
                        return null;
                    }
                }

                if (domainCategories.containsKey(hostName))
                {
                    return domainCategories.get(hostName);
                } else
                {
                    // no category match for this url
                    context.getCounter(ParseStats.PAGE_NO_CATEGORY).increment(1);
                }

            } catch (MalformedURLException e)
            {
                // java.net url parser gives up
                context.getCounter(ParseStats.ERR_URI_MALFORMED).increment(1);
                return null;

            } catch (IllegalArgumentException e)
            {
                //this is an IP address or an illegal URL e.g. parts starting or ending with _ or -
                context.getCounter(ParseStats.ERR_URI_ILLEGAL).increment(1);
                return null;

            } catch (NullPointerException e)
            {
                //Error reading uri.getHost()
                context.getCounter(ParseStats.ERR_URI_PARSE).increment(1);
                return null;

            } catch(IllegalStateException e)
            {
                // Not under public suffix
                context.getCounter(ParseStats.ERR_URI_NO_SUFFIX).increment(1);
                return null;
            }
            return null;
        }

        private void logNumCategories(Context context, int numCategories)
        {
            if (numCategories == 1)
            {
                context.getCounter(CategoryStats.ONE).increment(1);

            } else if (numCategories == 2)
            {
                context.getCounter(CategoryStats.TWO).increment(1);

            } else if (numCategories == 3)
            {
                context.getCounter(CategoryStats.THREE).increment(1);

            } else if (numCategories == 4)
            {
                context.getCounter(CategoryStats.FOUR).increment(1);

            } else if (numCategories == 5)
            {
                context.getCounter(CategoryStats.FIVE).increment(1);

            } else if (numCategories > 5)
            {
                context.getCounter(CategoryStats.MORE).increment(1);
            }
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

            ArrayList<String> categories = getCategories(url.toString(), context);

            if (categories != null) {
                String[] titleKeywords = getTitleKeywords(metadataText.toString(), context);
                if (titleKeywords != null) {
                    String keywords = titleKeywords[1] == null ? "" : titleKeywords[1];
                    context.write(url, new Text(String.format("%1$s\t%2$s\t%3$s", titleKeywords[0], keywords, joiner.join(categories))));
                    context.getCounter(ParseStats.SUCCESS).increment(1);

                    logNumCategories(context, categories.size());
                }
            }
        }
    }

    public static class TitleHarvestReducer extends Reducer<Text, Text, Text, Text>
    {
        private void logNumOccurrences(Context context, int numOccurrences)
        {
            if (numOccurrences == 1)
            {
                context.getCounter(URLOccurrence.ONE).increment(1);

            } else if (numOccurrences == 2)
            {
                context.getCounter(URLOccurrence.TWO).increment(1);

            } else if (numOccurrences == 3)
            {
                context.getCounter(URLOccurrence.THREE).increment(1);

            } else if (numOccurrences == 4)
            {
                context.getCounter(URLOccurrence.FOUR).increment(1);

            } else if (numOccurrences == 5)
            {
                context.getCounter(URLOccurrence.FIVE).increment(1);

            } else if (numOccurrences > 5)
            {
                context.getCounter(URLOccurrence.MORE).increment(1);
            }
        }

        @Override
        public void reduce(Text url, Iterable<Text> values, Context context) throws InterruptedException, IOException
        {
            int urlOccurrence = 0;

            for (Text value : values)
            {
                String valueStr = value.toString();

                context.write(url, new Text(valueStr));
                context.getCounter(ParseStats.SUCCESS).increment(1);

                urlOccurrence += 1;
            }

            logNumOccurrences(context, urlOccurrence);
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

            System.err.printf("Usage: %s [generic options] <segment_file_path> <output_path> [--region-code regionCode] [--variation variation]\n", getClass().getSimpleName());

            ToolRunner.printGenericCommandUsage(System.err);

            return -1;

        }


        // Get region code and cat index from arguments
        String regionCode = DEFAULT_REGION_CODE;
        String variation = "";
        if (args.length > 2) {
            for (int i = 0; i < args.length; i++) {
                if ("--region-code".equals(args[i]) && i < args.length - 1) {
                    regionCode = args[i + 1];
                } else if ("--variation".equals(args[i]) && i < args.length -1) {
                	variation = args[i + 1];
                }
            }
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

        Configuration conf = job.getConfiguration();

        // Set regionCode and variation for indentifying the categories file we should parse later.
        conf.set("titleHarvester.regionCode", regionCode);
        conf.set("titleHarvester.variation", variation);

        // Allows some (50%) of tasks fail; we might encounter the
        // occasional troublesome set of records and skipping a few
        // of 1000s won't hurt counts too much.
        conf.set("mapred.max.map.failures.percent", "50");

        // Compress the intermediate results from the map tasks
        conf.set("mapred.compress.map.output", "true");
        conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");

        // Runs the job.
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
