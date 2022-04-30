package exer1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class Exer1 {
    /**
     * The mapper reads the file, and emits pairs of numeronyms
     */
    public static class Exer1Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private final Text numeronyms = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("\\p{Punct}", " "));

            while (itr.hasMoreTokens()) {
                String str = itr.nextToken().trim().replaceAll("[^a-zA-Z ]", "").toLowerCase();
                char[] ch=str.toCharArray();
                if(ch.length>2) {
                    String numeronym = Character.toString(ch[0]) + ((ch.length) - 2) + Character.toString(ch[ch.length - 1]);
                    numeronyms.set(numeronym);
                    context.write(numeronyms, one);

                }
            }
        }
    }

    /**
     * This is the classic summary reducer which is used in the word count example
     */
    public static class Exer1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            // load the number of appearances parameter
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            // check that the number of appearances is higher than the sum.
            if (sum >= k) {
                result.set(sum);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        int k = 10;

        Configuration conf = new Configuration();
        conf.setInt("k", k);
        Job job = Job.getInstance(conf, "Exer1");
        job.setJarByClass(Exer1.class);
        job.setMapperClass(Exer1Mapper.class);
        job.setReducerClass(Exer1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

