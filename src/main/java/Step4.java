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


public class Step4 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        Text new_key = new Text();
        Text count = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
            while (lineItr.hasMoreTokens()) {
                String[] words = lineItr.nextToken().split("\\s+");

                //If words is a bi-gram, send it with the correct format: {(decade w1 w2):count}
                if(words.length>2){

                    //words = [w1,w2,decade,count]
                    context.write(new Text(words[2] + " " + words[0] + " " + words[1]),new Text(words[0] + " " + words[1]+ " "+ words[3]));
                }
                //If it's a decade count, remain the same {decade:count}
                else{

                    //words = [decade,count]
                    context.write(new Text(words[0]),new Text(words[1]));
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,DoubleWritable> {
        DoubleWritable decade_count = new DoubleWritable();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] keys = key.toString().split("\\s+");
            //Check if we got a decade count row
            if(keys.length == 1){
                decade_count.set(Double.parseDouble(values.iterator().next().toString()));
            }
            //Bi-gram case: calculate and subtract the appropriate values
            if(keys.length == 3 && !keys[0].equals("1")){
                for (Text value : values) {
//                  arrays keys and valueSplit will contain:
//                  keys=[decade,w1,w2]
//                  value=count
//                  where the original bi-gram is 'w1 w2' and of amount count.
                    context.write(new Text(keys[1] + " " + keys[2] + " " + keys[0]),
                            new DoubleWritable(Double.parseDouble(value.toString()) + decade_count.get()));
//                  It will send to the context - {(w1,w2,decade):(log(c(w1,w2))-log(c(w1))-log(c(w2))+log(N)}
                }
            }
        }
    }


    //Partitioner that compare only the first word of the key
    public static class PartitionerClass extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            String[] words = key.toString().split("\\s+");
            return ((words[0]).hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 2 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2gram count");
        job.setJarByClass(Step4.class);
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

        FileInputFormat.addInputPath(job, new Path("s3://dsp-2gram/output_step3_2gram_count.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-2gram/output_step4_2gram_count.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}