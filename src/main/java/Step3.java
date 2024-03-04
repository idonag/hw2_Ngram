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


public class Step3 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
            while (lineItr.hasMoreTokens()) {
                String[] words = lineItr.nextToken().split("\\s+");
                //Second word case, wright to context {(w2,decade):count}
                if(words[0].equals("1")){
                    //words = ['1',w2,decade,count]
                    context.write(new Text(words[1] + " " + words[2]),new Text(words[3]));
                }
                //Bi-gram case, wright to context {(w2,decade,'b'):(w1,count)}
                else if(words.length>2){
                    //words = [w1,w2,decade,count]
                    context.write(new Text(words[1] + " " + words[2] + " " + "b"),new Text(words[0] + " " + words[3]));
                }
                //Decade case, wright to context without change {decade:count}
                else{
                    context.write(new Text(words[0]),new Text(words[1]));
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,DoubleWritable> {
        DoubleWritable w2_count = new DoubleWritable();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] keys = key.toString().split("\\s+");
            //Second word case, save the count value into w2_count var
            if(keys.length == 2){
                w2_count.set(Double.parseDouble(values.iterator().next().toString()));
            }
            //Bi-gram case, calculate count with w2_count var
            else if(keys.length > 1){
                for (Text value : values) {
                    String[] valueSplit = value.toString().split("\\s+");

//                  arrays keys and valueSplit will contain:
//                  keys=[w2,decade,'b']
//                  valueSplit=[w1,count]
//                  where the original bi-gram is 'w1 w2' and of amount count.

                    context.write(new Text(valueSplit[0] + " " + keys[0] + " " + keys[1]),
                            new DoubleWritable(Double.parseDouble(valueSplit[1]) - w2_count.get()));

//                  It will send to the context - {(w1,w2,decade):(log(c(w1,w2))-log(c(w1))-log(c(w2)))}
                }
            }
            //Decade case, remain the same
            else{
                context.write(new Text(keys[0]),
                        new DoubleWritable(Double.parseDouble(values.iterator().next().toString())));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 3 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2gram count");
        job.setJarByClass(Step3.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(Step2.PartitionerClass.class);
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
///TODO change the file path for input and output!!!!!!!!!!
        FileInputFormat.addInputPath(job, new Path("s3://dsp-2gram/output_step2_2gram_count.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-2gram/output_step3_2gram_count.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}