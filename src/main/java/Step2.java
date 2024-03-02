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
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        Text new_key = new Text();
        Text count = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
            while (lineItr.hasMoreTokens()) {
                String[] words = lineItr.nextToken().split("\\s+");
                if(words[0].equals("0")){
                    context.write(new Text(words[1] + " " + words[2]),new Text(words[3]));
                }
                else if(!words[0].equals("1")){
                    context.write(new Text(words[0] + " " + words[2] + " " + "b"),new Text(words[1] + " " + words[3]));
                }
                else{
                    StringBuilder builder = new StringBuilder();
                    for (int i = 0; i < words.length - 1; i++) {
                        builder.append(words[i]);
                        if (i < words.length - 2) {
                            builder.append(" ");
                        }
                    }
                    new_key.set(builder.toString());
                    count.set(words[words.length - 1]);
                    context.write(new_key, count);
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,IntWritable> {
        /*
        hello word 1990 4
        0 hello 1990 12
        1 word 1990 6
         */

        IntWritable count = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] keys = key.toString().split("\\s+");
            if(keys.length == 2){
                count.set(Integer.parseInt(values.iterator().next().toString()));
            }
            else if(keys[0].equals("1")){
                context.write(key,new IntWritable(Integer.parseInt(values.iterator().next().toString())));
            }
            else{
                for (Text value : values) {
                    String[] valueSplit = value.toString().split("\\s+");
                    context.write(new Text(keys[0] + " " + valueSplit[0] + " " + keys[1]),
                            new IntWritable(Integer.parseInt(valueSplit[1]) - count.get()));
                }
            }
        }
    }

    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] words = key.toString().split("\\s+");
            return ((words[0]+ " " +words[1]).hashCode()) % numPartitions; //hello word 1990 -> 0 hello 1990
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2gram count");
        job.setJarByClass(Step2.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(PartitionerClass.class);
        job.setCombinerClass(ReducerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

        FileInputFormat.addInputPath(job, new Path("s3://dsp-2gram/output_2gram_count.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-2gram/output_step2_2gram_count.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}