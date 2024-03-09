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


public class Step3 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
            while (lineItr.hasMoreTokens()) {
                String[] words = lineItr.nextToken().split("\\s+");
                //Second word case, write to context {(decade,w2):log(count)}
                if(words[1].equals("1")){
                    //words = [decade,'1',w2,log(count)]
                    context.write(new Text(words[0] + " " + words[2]),new Text(words[3]));
                }
                //Bi-gram case, write to context {(decade,w2,'b'):(w1,log(count),count)}
                else if(words.length>2){
                    //words = [decade,w1,w2,log(count),count]
                    context.write(new Text(words[0] + " " + words[2] + " " + "b"),new Text(words[1] + " " + words[3] + " " + words[4]));
                }
                //Decade case, write to context without change {decade:count}
                else{
                    context.write(new Text(words[0]),new Text(words[1]));
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {
        DoubleWritable w2_count = new DoubleWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] keys = key.toString().split("\\s+");
            //decade case - remain the same
            if (keys.length == 1) {
                context.write(key, values.iterator().next());
            }
            else if (keys.length == 2) {
                w2_count.set(Double.parseDouble(values.iterator().next().toString()));
            }
            else {
                writeBigram(keys, key, values, context);
            }
        }

        private void writeBigram(String[] keys, Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
//                  arrays keys and valueSplit will contain:
//                  keys=[decade,w2,'b']
//                  valueSplit=[w1,log(count),count]
//                  where the original bi-gram is 'w1 w2' and of amount count.
                String[] valueSplit = value.toString().split("\\s+");
                Text newKey = new Text(keys[0]+ " " + valueSplit[0] + " "+ keys[1]);
                double logCount = Double.parseDouble(valueSplit[1]) - w2_count.get();
                Text newVal = new Text(logCount + " " + valueSplit[2]);
                context.write(newKey, newVal);
//                  It will send to the context - {(decade,w1,w2):(log(c(w1,w2)) - log(c(w1)) - log(c(w2)),count)}
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
        job.setPartitionerClass(TwoGrams.PartitionerClass.class);
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
///TODO change the file path for input and output!!!!!!!!!!
        FileInputFormat.addInputPath(job, new Path("s3://dsp-2gram2/output_step2_2gram_count.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-2gram2/output_step3_2gram_count.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    // {1,w2,dec}:log(count) -----> NULL
    //decade:N ----> decade:N
    //{w1,w1,dec}:log(count)-log(c(w1)) -----> {w1,w2,dec}:log(count)-log(c(w1))-log(c(w2))


}