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
import java.util.StringTokenizer;


public class Step5 {
    public static class MapperClass extends Mapper<LongWritable, Text, Text, Text> {
        Text new_key = new Text();
        Text count = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
            while (lineItr.hasMoreTokens()) {
                String[] words = lineItr.nextToken().split("\\s+");
                if(words.length == 2){
                    //words = {dec,npmi_count}
                    context.write(new Text(words[0]),new Text(words[1]));
                }
                else{
                //words = {dec,npmi,w1,w2}
                context.write(new Text(words[0]+ " " + words[1]), new Text(words[2] + " " + words[3]));
                //sends to context - {dec,npmi}:{w1,w2}
                }
            }
        }
    }

    public static class ReducerClass extends Reducer<Text,Text,Text,Text> {

        DoubleWritable npmi_sum = new DoubleWritable();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,  InterruptedException {
            String[] keySplit = key.toString().split("\\s+");
            if(keySplit.length == 1){
                npmi_sum.set(Double.parseDouble(values.iterator().next().toString()));
            }
            else {
                for (Text value : values) {
                    if(isCollocation(Double.parseDouble(keySplit[1]),context)) {
                        Text newKey = new Text(keySplit[0]+" "+value);
                        Text newVal = new Text(keySplit[1]);
                        context.write(newKey, newVal);
                    }
                }
            }
        }
        private boolean isCollocation(double npmi,Context context) {
            double min_pmi = Double.parseDouble(context.getConfiguration().get("min_pmi","1"));
            double rel_min_pmi = Double.parseDouble(context.getConfiguration().get("rel_min_pmi","1"));
            double rel_npmi = npmi/npmi_sum.get();
            return npmi >= min_pmi || rel_npmi >= rel_min_pmi;
        }
    }

    public static class DescendingComparator extends Text.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return super.compare(b2, s2, l2, b1, s1, l1);
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[DEBUG] STEP 5 started!");
        System.out.println(args.length > 0 ? args[0] : "no args");
        Configuration conf = new Configuration();
        conf.set("min_pmi",args[0]);
        conf.set("rel_min_pmi",args[1]);
        Job job = Job.getInstance(conf, "2gram count");
        job.setJarByClass(Step5.class);
        job.setMapperClass(MapperClass.class);
        job.setPartitionerClass(TwoGrams.PartitionerClass.class);
        job.setReducerClass(ReducerClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setSortComparatorClass(DescendingComparator.class);
        job.setNumReduceTasks(App.numOfReducers);

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setInputFormatClass(SequenceFileInputFormat.class);
//        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-us-all/3gram/data"));

        FileInputFormat.addInputPath(job, new Path("s3://dsp-2gram2/output_step4_2gram_count.txt"));
        FileOutputFormat.setOutputPath(job, new Path("s3://dsp-2gram2/output_step5_2gram_count.txt"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}