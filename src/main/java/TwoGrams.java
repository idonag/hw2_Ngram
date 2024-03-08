
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import com.amazonaws.auth.AWSCredentialsProvider;

public class TwoGrams {
    public static String getObjectFromBucket(String bucketName, String keyName) throws IOException {
        AmazonS3 S3;
        AWSCredentialsProvider credentialsProvider;
        credentialsProvider = new ProfileCredentialsProvider("C:\\Users\\idona\\.aws\\config.txt","default");
        S3 = AmazonS3ClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();

        GetObjectRequest request = new GetObjectRequest(bucketName,keyName);
        S3Object s3Object = S3.getObject(request);
        S3ObjectInputStream s3ObjectInputStream = s3Object.getObjectContent();
        String curr_dir = System.getProperty("user.dir");
        String destination_path = curr_dir + File.separator+keyName;
        FileOutputStream fileOutputStream = new FileOutputStream(destination_path);

        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = s3ObjectInputStream.read(buffer)) != -1) {
            fileOutputStream.write(buffer, 0, bytesRead);
        }
        fileOutputStream.close();
        return destination_path;
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
        private Set<String> stopWords;
        private Text bigram = new Text();
        private Text bigramKey = new Text();
        private Text w1Key = new Text();
        private Text w2Key = new Text();
        public MapperClass(){
            super();
            this.stopWords = new HashSet<>();
            loadStopWords(stopWords);
        }

        private void loadStopWords(Set<String> stopWords) {
            try {
                String filePath = getObjectFromBucket("dsp-2gram2","eng-stopwords.txt");
                fileToSet(filePath,stopWords);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }



        private boolean isPotentialCalloc(Text bigram){
            String[] words = bigram.toString().split("\\s+");
            if(words.length != 2)
                return false;
            if (!words[0].matches("[a-zA-Z]+") || !words[1].matches("[a-zA-Z]+"))
                return false;
            return !stopWords.contains(words[0]) && !stopWords.contains(words[1]);
        }
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
            while (lineItr.hasMoreTokens()) {
                StringTokenizer itr = new StringTokenizer(lineItr.nextToken(), "\t");
                int i = 0;
                while (itr.hasMoreTokens() && i < 3) {
                    switch (i) {
                        case 0:
                            bigram.set(itr.nextToken());
                            break;
                        case 1:
                            decade.set((Integer.parseInt(itr.nextToken()) /10) * 10);
                            break;
                        case 2:
                            count.set(Integer.parseInt(itr.nextToken()));
                            break;
                    }
                    i++;
                }
                if(isPotentialCalloc(bigram)) {
                    bigramKey.set(bigram.toString() + " " + decade.get());
                    context.write(bigramKey, new Text(count.toString()));
                    String[] words = bigram.toString().split(" ");
                    w1Key.set("0 " + words[0] + " " + decade);
                    w2Key.set("1 " + words[1] + " " + decade);
                    context.write(w1Key, new Text(count.toString()));
                    context.write(w2Key, new Text(count.toString()));
                    context.write(new Text(String.valueOf(decade.get())), new Text(count.toString()));
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
            if(keys.length == 1 || !(keys[0].equals("0") || keys[0].equals("1"))){
                context.write(key,new Text(String.valueOf(sum)));
            }
            else {
                context.write(key, new Text(String.valueOf(logSum)));
            }

        }
    }

    public static class PartitionerClass extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            return key.hashCode() % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 1 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2gram count");
        job.setJarByClass(TwoGrams.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.out.println("job configured!");


//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

        FileInputFormat.addInputPath(job, new Path("s3://dsp-2gram2/2gram_short.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-2gram2/output_2gram_count.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
