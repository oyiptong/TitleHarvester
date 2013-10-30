package org.mozilla.up;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.net.InternetDomainName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.LangDetectException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * Created with IntelliJ IDEA.
 * User: oyiptong
 * Date: 2013-06-13
 * Time: 4:00 PM
 */
public class TextHarvester extends Configured implements Tool
{
    enum ParseStats
    {
        ERR_SETUP,
        ERR_LANG_DETECT,
        ERR_URI_MALFORMED,
        ERR_URI_ILLEGAL,
        ERR_URI_PARSE,
        ERR_URI_NO_SUFFIX,
        SUCCESS,
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

    public static class TextHarvestMapper extends Mapper<Text, Text, Text, Text>
    {
        private static HashSet<String> domainWhitelist;
        private static HashSet<String> stopwords;
        private static CharMatcher tokenMatcher = CharMatcher.WHITESPACE
                .or(CharMatcher.JAVA_LETTER_OR_DIGIT.negate())
                .and(CharMatcher.isNot('\''));
        private static Joiner tokenJoiner = Joiner.on(" ").skipNulls();

        /**
         * Obtains the top private domain of a given url. e.g. games.1up.com begets 1up.com
         * @param url input url as a string
         * @param context hadoop context
         * @return top private domain of a url
         */
        private String getDomain(String url, Context context)
        {
            try
            {
                URL uri = new URL(url.toLowerCase());
                InternetDomainName domainName = InternetDomainName.from(uri.getHost());
                String domainStr = domainName.topPrivateDomain().name();

                return domainStr;

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
        }

        /**
         * Cleans up text to make it ready for further processing
         * @param input input text
         * @return a stripped, stop-worded version of the given input
         */
        private String getCleanedText(String input)
        {
            ArrayList<String> outTokens = new ArrayList<String>();

            // tokenize
            Iterable<String> inTokens = Splitter.on(tokenMatcher)
                    .trimResults(CharMatcher.JAVA_LETTER.negate())
                    .omitEmptyStrings()
                    .split(input);

            HashMultiset<String> tokenCounts = HashMultiset.create();
            for (String token: inTokens)
            {
                token = token.toLowerCase();
                if (!stopwords.contains(token))
                {
                    tokenCounts.add(token);
                }
            }

            for (Multiset.Entry<String> entry: tokenCounts.entrySet())
            {
                outTokens.add(String.format("%1$s:%2$d", entry.getElement(), entry.getCount()));
            }

            return tokenJoiner.join(outTokens);
        }

        private void setupLangDetector(Context context) throws LangDetectException
        {
            try
            {
                File profileDir = new File(getClass().getResource("/data/profiles").getFile());
                DetectorFactory.loadProfile(profileDir);
            } catch (LangDetectException e)
            {
                context.getCounter(ParseStats.ERR_SETUP).increment(1);
                throw e;
            }
        }

        private void setupStopwordList(Context context) throws IOException
        {
            try
            {
                stopwords = new HashSet<String>();
                InputStream is = getClass().getResourceAsStream("/data/stop-words/stop-words-english.txt");
                BufferedReader reader = new BufferedReader(new InputStreamReader(is));
                String line;
                while ((line = reader.readLine()) != null)
                {
                    stopwords.add(line);
                }
            } catch (IOException e)
            {
                context.getCounter(ParseStats.ERR_SETUP).increment(1);
                throw e;
            }
        }

        @Override
        protected void setup(Context context) throws InterruptedException, IOException {
            super.setup(context);
            try
            {
                setupLangDetector(context);
                setupStopwordList(context);
            } catch (LangDetectException e)
            {
                //UGLY: can't throw LangDetectException due to interface, convert to IOException
                throw new IOException(e.toString());
            }

            ObjectMapper mapper = new ObjectMapper();
            ArrayList<String> domains = mapper.readValue(getClass().getResourceAsStream("/data/textData_host_whitelist.json"), new TypeReference<ArrayList<String>>(){});
            domainWhitelist = new HashSet<String>(domains);
        }

        @Override
        public void map(Text url, Text text, Context context) throws InterruptedException, IOException
        {
            try
            {
                Detector langDetector = DetectorFactory.create();
                langDetector.append(text.toString());
                String lang = langDetector.detect();

                // discard non-english for now
                if (lang.equals("en"))
                {
                    String domain = getDomain(url.toString(), context);
                    if (domainWhitelist.contains(domain))
                    {
                        String cleanedText = getCleanedText(text.toString());
                        context.write(url, new Text(String.format("%1$s\t%2$s", domain, cleanedText)));
                    }
                }

                // send language for counting
                context.write(new Text("0lang:"+lang), new Text(""));

            } catch (LangDetectException e)
            {
                context.getCounter(ParseStats.ERR_LANG_DETECT).increment(1);
            }
        }
    }

    public static class TextHarvestReducer extends Reducer<Text, Text, Text, Text>
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
        public void reduce(Text key, Iterable<Text> values, Context context) throws InterruptedException, IOException
        {
            int urlOccurrence = 0;

            String keyStr = key.toString();
            if (keyStr.substring(0, 5).equals("0lang"))
            {
                int sum = 0;
                for (Text value : values)
                {
                    sum += 1;
                }
                context.write(key, new Text(Integer.toString(sum)));
            } else
            {
                // if text, store the longest text value in the case of multiple occurrences
                Text longestValue = new Text();
                for (Text value : values)
                {
                    if (value.getLength() > longestValue.getLength())
                    {
                        longestValue = value;
                    }
                    urlOccurrence += 1;
                }
                context.write(key, longestValue);
                context.getCounter(ParseStats.SUCCESS).increment(1);
                logNumOccurrences(context, urlOccurrence);
            }

        }
    }

    public static void main(String[] args) throws Exception
    {
        int res = ToolRunner.run(new Configuration(), new TextHarvester(), args);
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

        job.setMapperClass(TextHarvestMapper.class);
        job.setReducerClass(TextHarvestReducer.class);

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
            String segmentPath = String.format("%1$s/aws-publicdatasets/common-crawl/parse-output/segment/%2$s/textData-*", pathPrefix, line);
            System.out.println(String.format("Adding file at: %1$s", segmentPath));

            FileInputFormat.addInputPath(job, new Path(segmentPath));
            line = br.readLine();
        }

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Configuration conf = job.getConfiguration();
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
