import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;


public class InvertedIndexer {

    public static class InvertedIndexerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        static enum CountersEnum { INPUT_WORDS }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private Set<String> patternsToSkip = new HashSet<String>();
        private Set<String> punctuations = new HashSet<String>();

        private Configuration conf;
        private BufferedReader fis;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();

            if (conf.getBoolean("wordcount.skip.patterns", false)) {
                URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();

                Path patternsPath = new Path(patternsURIs[0].getPath());
                String patternsFileName = patternsPath.getName().toString();
                parseSkipFile(patternsFileName);

                Path punctuationsPath = new Path(patternsURIs[1].getPath());
                String punctuationsFileName = punctuationsPath.getName().toString();
                parseSkipPunctuations(punctuationsFileName);
            }
        }

        /**
         * ?????????filename?????????????????????????????????????????????????????????????????????patternsToSkip
         * @param fileName
         */
        private void parseSkipFile(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    patternsToSkip.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        /**
         * ?????????filename????????????????????????????????????????????????????????????????????????punctuations
         * @param fileName
         */
        private void parseSkipPunctuations(String fileName) {
            try {
                fis = new BufferedReader(new FileReader(fileName));
                String pattern = null;
                while ((pattern = fis.readLine()) != null) {
                    punctuations.add(pattern);
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while parsing the cached file "
                        + StringUtils.stringifyException(ioe));
            }
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String textName = fileSplit.getPath().getName();

            String line = value.toString().toLowerCase();
            for (String pattern : punctuations) {
                line = line.replaceAll(pattern, " ");
            }
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                String one_word = itr.nextToken();

                //????????????????????????3
                if(one_word.length()<3) {
                    continue;
                }
                //??????????????????????????????????????????
                if(Pattern.compile("^[-\\+]?[\\d]*$").matcher(one_word).matches()) {
                    continue;
                }
                //????????????????????????
                if(patternsToSkip.contains(one_word)){
                    continue;
                }

                word.set(one_word+"#"+textName); //key?????????#?????????????????????????????????????????????
                context.write(word, one);
                Counter counter = context.getCounter(
                        CountersEnum.class.getName(),
                        CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
            }
        }
    }

    /** ??????Combiner???Mapper??????????????????value??????????????????????????? **/
    public static class SumCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /** ?????????HashPartitioner????????? <term, docid>?????????key?????????term?????????Reducer **/
    public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = new String();
            term = key.toString().split("#")[0]; // <term#docid>=>term
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, NullWritable>{

        private Text word1 = new Text();
        private Text word2 = new Text();
        String temp = new String();
        static Text CurrentItem = new Text(" ");
        static List<String> postingList = new ArrayList<String>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            word1.set(key.toString().split("#")[0]); // word,key
            temp = key.toString().split("#")[1]; // filename
            for (IntWritable val : values) {
                sum += val.get();
            }
            word2.set(sum + "#" + temp); //??????a#??????i  ??????????????????
            if (!CurrentItem.equals(word1) && !CurrentItem.equals(" ")) { //CurrentItem!=word1, CurrentItem!=" "
                Collections.sort(postingList,Collections.reverseOrder());
                StringBuilder out = new StringBuilder();
                int len = postingList.size();
                int i = 0;
                int count = 0;
                for (String p : postingList) {
                    String docId = p.toString().split("#")[1];
                    String wordCount = p.toString().split("#")[0];
                    count += Integer.parseInt(wordCount);
                    out.append(docId + "#" + wordCount);
                    if(i != len-1){
                        out.append(", ");
                        i++;
                    }
                }
                if(count != 0)
                    context.write(new Text(CurrentItem+": "+out.toString()), NullWritable.get());
                postingList = new ArrayList<String>();
            }
            CurrentItem = new Text(word1);
            postingList.add(word2.toString()); // ?????????postingList????????????????????????????????????
        }

        // cleanup ?????????????????????????????????cleanup???????????????????????????????????????
        public void cleanup(Context context) throws IOException,
                InterruptedException {
            Collections.sort(postingList,Collections.reverseOrder());
            StringBuilder out = new StringBuilder();
            int len = postingList.size();
            int i = 0;
            int count = 0;
            for (String p : postingList) {
                String docId = p.toString().split("#")[1];
                String wordCount = p.toString().split("#")[0];
                count += Integer.parseInt(wordCount);
                out.append(docId + "#" + wordCount);
                if(i != len-1){
                    out.append(", ");
                    i++;
                }
            }
            if(count != 0)
                context.write(new Text(CurrentItem+": "+out.toString()), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);

        String[] remainingArgs = optionParser.getRemainingArgs();

        if (remainingArgs.length != 5) {
            System.err.println("Usage: wordcount <in> <out> -skip punctuations skipPatternFile");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "Inverted Indexer");
	job.setJar("invertedindexer.jar");
        job.setJarByClass(InvertedIndexer.class);
        job.setMapperClass(InvertedIndexerMapper.class);
        job.setCombinerClass(SumCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setPartitionerClass(NewPartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        List<String> otherArgs = new ArrayList<String>(); // ?????? -skip ?????????????????????
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
                job.addCacheFile(new Path(remainingArgs[++i]).toUri()); // ??? -skip ?????????????????????skip???????????????url???????????????????????????
                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true); // ???????????????wordcount.skip.patterns????????????mapper?????????
            } else {
                otherArgs.add(remainingArgs[i]); // ????????? -skip ???????????????????????????otherArgs???
            }
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
