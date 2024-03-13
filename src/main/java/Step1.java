
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class Step1 {
    static boolean isEnglish;
    public static String getObjectFromBucket(String key_name,String bucket_name){
        System.out.format("Downloading %s from S3 bucket %s...\n", key_name, bucket_name);
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1).build();
        try {
            S3Object o = s3.getObject(bucket_name, key_name);
            S3ObjectInputStream s3is = o.getObjectContent();
            FileOutputStream fos = new FileOutputStream(key_name);
            byte[] read_buf = new byte[1024];
            int read_len;
            while ((read_len = s3is.read(read_buf)) > 0) {
                fos.write(read_buf, 0, read_len);
            }
            s3is.close();
            fos.close();
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.exit(1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
        return key_name;
    }

    private static void fileToSet(String filePath, Set<String> stopWords) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] words = line.split("\\s+"); // Split by whitespace characters
                // Add the word to the set
                stopWords.addAll(Arrays.asList(words));
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
    }

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable count = new IntWritable();
        private final static IntWritable decade = new IntWritable();
        private final Set<String> stopWordsHeb;
        private final Set<String> stopWordsEng;
        private final Text bigram = new Text();
        private final Text bigramKey = new Text();
        private final Text w1Key = new Text();
        private final Text w2Key = new Text();
        public MapperClass(){
            super();
            this.stopWordsHeb = new HashSet<>();
            this.stopWordsEng = new HashSet<>();
//            String keyName;
//            if(isEnglish){
//                keyName = "eng-stopwords.txt";
//            }
//            else{
//                keyName = "heb-stopwords.txt";
//            }
            //String keyName = isEnglish ? "eng-stopwords.txt" : "heb-stopwords.txt";
            //loadStopWords(stopWords,keyName);
            //loadStopWords(stopWords,"eng-stopwords.txt");
            loadStopWords(stopWordsHeb,"heb-stopwords.txt");
            loadStopWords(stopWordsEng,"eng-stopwords.txt");
        }

        private void loadStopWords(Set<String> stopWords,String keyName) {
            try {
                String filePath = getObjectFromBucket(keyName,"dsp-2gram");
                fileToSet(filePath,stopWords);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }


        private boolean isPotentialCalloc(Text bigram, Context context){
            String[] words = bigram.toString().split("\\s+");
            String language = context.getConfiguration().get("isEnglish","E");
            boolean isEnglish = language.equals("E");
            if(words.length != 2)
                return false;
            if(isEnglish) {
                if (!words[0].matches("[a-zA-Z]+") || !words[1].matches("[a-zA-Z]+"))
                    return false;
                return !stopWordsEng.contains(words[0].toLowerCase()) && !stopWordsEng.contains(words[1].toLowerCase());
            }
            else{
                if(!words[0].matches("[א-ת]+") || !words[1].matches("[א-ת]+"))
                    return false;
                return !stopWordsHeb.contains(words[0]) && !stopWordsHeb.contains(words[1]);
            }
        }
        @Override
        public void map(LongWritable key, Text value, Context context){
            StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
            while (lineItr.hasMoreTokens()) {
                try {
                    String[] splitWords = lineItr.nextToken().split("\\s+");

                    bigram.set(splitWords[0] + " " + splitWords[1]);
                    decade.set((Integer.parseInt(splitWords[2]) / 10) * 10);
                    count.set(Integer.parseInt(splitWords[3]));

                    if (isPotentialCalloc(bigram,context)) {
                        bigram.set(bigram.toString().toLowerCase());
                        bigramKey.set(decade.get() + " " + bigram);
                        context.write(bigramKey, new Text(count.toString()));
                        String[] words = bigram.toString().split(" ");
                        w1Key.set(decade.get() + " 0 " + words[0]);
                        w2Key.set(decade.get() + " 1 " + words[1]);
                        context.write(w1Key, new Text(count.toString()));
                        context.write(w2Key, new Text(count.toString()));
                        context.write(new Text(String.valueOf(decade.get())), new Text(count.toString()));
                    }
                }
                catch (Exception e){
                    System.out.printf("Error while parsing:%s%n",value);
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            double sum = 0;
            for (Text value : values) {
                sum += Double.parseDouble(value.toString());
            }
            double logSum = Math.log(sum);
            String[] keys = key.toString().split("\\s+");
            if(keys.length == 1 || (sum > 2 && (!(keys[1].equals("0") || keys[1].equals("1"))))){
                context.write(key,new Text(String.valueOf(sum)));
            }
            else if(keys[1].equals("0") || keys[1].equals("1")){
                context.write(key, new Text(String.valueOf(logSum)));
            }

        }
    }
    public static class CombinerClass extends Reducer<Text,Text,Text,Text>{
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            double sum = 0;
            for (Text value : values) {
                sum += Double.parseDouble(value.toString());
            }
            context.write(key,new Text(String.valueOf(sum)));
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] words = key.toString().split("\\s+");
            int int_key = Integer.parseInt(words[0]) / 10;
            return int_key % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        isEnglish = args[0].equals("E");
        System.out.println("is english:\t" + isEnglish);
        conf.set("isEnglish",args[0]);
        /*conf.set("mapred.max.split.size",)*/
        Job job = Job.getInstance(conf, "2gram count");
        job.setJarByClass(Step1.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setCombinerClass(CombinerClass.class);
        job.setNumReduceTasks(App.numOfReducers);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        System.out.println("job configured!");

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));
        if(isEnglish){
            FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data"));
        }
        else{
            FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
        }

        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-2gram/output_step1_2gram_count.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
