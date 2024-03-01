import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
public class TwoGrams {


    //Writable class that represent each key of bigram - decade
    public static class BigramPerDecade implements WritableComparable {
        private String bigram;
        private int decade;

        public BigramPerDecade(){

        }

        public BigramPerDecade(Text bigram, IntWritable year){
            setBigram(bigram);
            setYear(year);
        }
        @Override
        public void readFields(DataInput dataInput) throws IOException {
            bigram = dataInput.readUTF();
            decade = dataInput.readInt();
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(bigram);
            dataOutput.writeInt(decade);
        }

        public void setYear(IntWritable year) {
            //Calculate the decade of -year-
            this.decade = (year.get()/10) * 10;
        }

        public void setBigram(Text bigram) {
            this.bigram = bigram.toString();
        }

        @Override
        public int compareTo(Object other) {
            if(!(other instanceof BigramPerDecade))
                return 1;
            BigramPerDecade otherBigram = (BigramPerDecade)other;
            // First, compare by decade
            int decadeComparison = Integer.compare(this.decade, otherBigram.decade);
            if (decadeComparison != 0) {
                return decadeComparison;
            }
            // If decades are equal, compare by bigram
            return this.bigram.compareTo(otherBigram.bigram);
        }
    }

    public static class MapperClass extends Mapper<LongWritable, Text, BigramPerDecade, IntWritable> {
        private final static IntWritable count = new IntWritable();
        private final static IntWritable year = new IntWritable();
        private Text bigram = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            StringTokenizer lineItr = new StringTokenizer(value.toString(), "\n");
            while (lineItr.hasMoreTokens()) {
                StringTokenizer itr = new StringTokenizer(lineItr.nextToken(), "\t");
                int i = 0;
                while (itr.hasMoreTokens()) {
                    switch (i) {
                        case 0:
                            bigram.set(itr.nextToken());
                            break;
                        case 1:
                            year.set(Integer.parseInt(itr.nextToken()));
                            break;
                        case 2:
                            count.set(Integer.parseInt(itr.nextToken()));
                            break;
                    }
                    i++;
                }
                BigramPerDecade bpy = new BigramPerDecade();
                bpy.setYear(year);
                bpy.setBigram(bigram);
                context.write(bpy, count);
            }
        }
    }

    public static class ReducerClass extends Reducer<BigramPerDecade,IntWritable,BigramPerDecade,IntWritable> {
        @Override
        public void reduce(BigramPerDecade key, Iterable<IntWritable> values, Context context) throws IOException,  InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static class PartitionerClass extends Partitioner<BigramPerDecade, IntWritable> {
        @Override
        public int getPartition(BigramPerDecade key, IntWritable value, int numPartitions) {
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
        job.setMapOutputKeyClass(BigramPerDecade.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(BigramPerDecade.class);
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
