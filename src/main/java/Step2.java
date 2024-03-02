import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;


public class Step2 {
        public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
            Text new_key = new Text();
            IntWritable count = new IntWritable();
            @Override
            public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
                String[] words = value.toString().split(" ");
                StringBuilder builder = new StringBuilder();
                for (int i = 0; i < words.length; i++) {
                    builder.append(words[i]);
                    if (i < words.length - 1) {
                        builder.append(" ");
                    }
                }
                new_key.set(builder.toString());
                count.set(Integer.parseInt(words[words.length-1]));
                context.write(new_key,count);
            }
        }

        public static class ReducerClass extends Reducer<Text,IntWritable,Text,IntWritable> {
            /*
            hello word 1990 4
            0 hello 1990 12
            1 word 1990 6
             */
            @Override
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
                int sum = 0;
                for (IntWritable value : values) {
                    sum += value.get();
                }
                context.write(key, new IntWritable(sum));
            }
        }

        public static class PartitionerClass extends Partitioner<Text, IntWritable> {
            @Override
            public int getPartition(Text key, IntWritable value, int numPartitions) {
                if(!(key.toString().startsWith("0") || key.toString().startsWith("1"))){
                    return (("0 " + key.toString().split(" ")[0]+ " " +key.toString().split(" ")[2]).hashCode()) % numPartitions; //hello word 1990 -> 0 hello 1990
                }
                return key.hashCode() % numPartitions; // 0 hello 1990
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
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

            FileInputFormat.addInputPath(job, new Path("s3://dsp-2gram/2gram_short.txt"));
            FileOutputFormat.setOutputPath(job, new Path("s3://dsp-2gram/output_2gram_count.txt"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }

    }


