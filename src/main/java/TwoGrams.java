import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
public class TwoGrams {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable count = new IntWritable();
        private final static IntWritable decade = new IntWritable();
        private Text bigram = new Text();
        private Text bigramKey = new Text();
        private Text w1Key = new Text();
        private Text w2Key = new Text();

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
                bigramKey.set(bigram.toString() + " " + decade.get());
                context.write(bigramKey, new Text(count.toString()));
                String[] words = bigram.toString().split(" ");
                w1Key.set("0 " + words[0] + " " + decade);
                w2Key.set("1 " + words[1] + " " + decade);
                context.write(w1Key,new Text(count.toString()));
                context.write(w2Key,new Text(count.toString()));
                context.write(new Text(String.valueOf(decade.get())),new Text(count.toString()));
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
            System.out.println("key: "+key.toString()+"sum: "+sum);
            double log_sum = Math.log(sum);
            context.write(key, new Text(String.valueOf(sum)));
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
        job.setCombinerClass(ReducerClass.class);
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
