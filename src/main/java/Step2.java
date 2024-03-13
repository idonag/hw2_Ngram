import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


public class Step2 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        Text new_key = new Text();
        Text count = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
            while (lineItr.hasMoreTokens()) {
                String[] words = lineItr.nextToken().split("\\s+");
                //If words is a bi-gram, send it with the correct format: {(decade,w1,'b'):(w2,count)}
                if (!words[1].equals("1") && !words[1].equals("0") && words.length > 2) {
                    //words = [decade,w1,w2,count]
                    context.write(new Text(words[0] + " " + words[1] + " " + "b"), new Text(words[2] + " " + words[3]));
                }
                //If it's a first word representation, send it with the correct format: {(decade,w1) : count}
                else if (words[1].equals("0")) {
                    //words = [decade,'0',w1,count]
                    context.write(new Text(words[0] + " " + words[2]), new Text(words[3]));
                }
                //If it's a second word representation or a decade count, remain unchanged.
                else {
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

    public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
        DoubleWritable w1_count = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keys = key.toString().split("\\s+");
            if (keys.length == 1 || keys[1].equals("1")) {
                context.write(key, values.iterator().next());
            } else if (keys.length == 2) {
                w1_count.set(Double.parseDouble(values.iterator().next().toString()));
            } else {
                writeBigram(keys, key, values, context);
            }
        }

        private void writeBigram(String[] keys, Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
//                  arrays keys and valueSplit will contain:
//                  keys=[decade,w1,'b']
//                  valueSplit=[w2,count]
//                  where the original bi-gram is 'w1 w2' and of amount count.
                String[] valueSplit = value.toString().split("\\s+");
                    Text newKey = new Text(keys[0] + " " + keys[1] + " " + valueSplit[0]);
                    double numVal = calculateLog(Double.parseDouble(valueSplit[1]),w1_count.get());
                    Text newVal = new Text(numVal +" "+ valueSplit[1]);
                    context.write(newKey, newVal);
//                  It will send to the context - {(decade,w1,w2):(log(c(w1,w2))-log(c(w1)),c(w1w2))}
            }
        }
        private double calculateLog(double cw1w2, double logCw1){
            return Math.log(cw1w2) - logCw1;
        }
    }


        public static void main(String[] args) throws Exception {
            System.out.println("[DEBUG] STEP 2 started!");
            System.out.println(args.length > 0 ? args[0] : "no args");
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "2gram count");
            job.setJarByClass(Step2.class);
            job.setMapperClass(MapperClass.class);
            job.setPartitionerClass(Step1.PartitionerClass.class);
            job.setReducerClass(ReducerClass.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(App.numOfReducers);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

            FileInputFormat.addInputPath(job, new Path("s3://dsp-2gram/output_step1_2gram_count.txt"));
            FileOutputFormat.setOutputPath(job, new Path("s3://dsp-2gram/output_step2_2gram_count.txt"));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }

    }
