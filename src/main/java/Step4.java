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


public class Step4 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        Text new_key = new Text();
        Text count = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
            while (lineItr.hasMoreTokens()) {
                String[] words = lineItr.nextToken().split("\\s+");

                //If words is a bi-gram, send it with the correct format: {decade: w1,w2,log(count),count}
                if(words.length>2){
                    //words = [decade,w1,w2,log(count),count]
                    context.write(new Text(words[0]+ " b") , new Text(words[1] + " " + words[2]+  " " + words[3] + " " + words[4]));
                }
                //If it's a decade count, remain the same {decade:count}
                else{
                    //words = [decade,count]
                    context.write(new Text(words[0]),new Text(words[1]));
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        DoubleWritable decade_count = new DoubleWritable();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] keys = key.toString().split("\\s+");
            //Check if we got a decade count row
            if(keys.length == 1){
                decade_count.set(Double.parseDouble(values.iterator().next().toString()));
            }
            //Bi-gram case: calculate and subtract the appropriate values
            else {
                //keys = {dec,b}
                //value = {w1,w2,log(count),count}
                double npmi_sum = 0;
                for(Text value : values){
                    String[] valueSplit = value.toString().split("\\s+");
                    double almostPmi = Double.parseDouble(valueSplit[2]);
                    double cw1w2 = Double.parseDouble(valueSplit[3]);
                    double npmi = calculateNpmi(decade_count.get(),cw1w2,almostPmi);
                    Text newKey = new Text(keys[0] + " " + npmi + " " + valueSplit[0] + " " + valueSplit[1]);
                    context.write(newKey,new Text(""));
                    npmi_sum += npmi;
                }
                context.write(new Text(keys[0]),new Text(String.valueOf(npmi_sum)));
                //{dec,npmi,w1,w2}:""
                //{dec:npmi_sum}
            }
        }

        private double calculateNpmi(double N, double countW1W2,double almostPmi){
            double pmi = almostPmi + Math.log(N);
            double pw1w2 = countW1W2/N;
            if(pw1w2 == 1){
                return 1;
            }
            double denom = -Math.log(pw1w2);
            return pmi/denom;
        }
    }



    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 4 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2gram count");
        job.setJarByClass(Step4.class);
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

        FileInputFormat.addInputPath(job, new Path("s3://dsp-2gram/output_step3_2gram_count.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-2gram/output_step4_2gram_count.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}