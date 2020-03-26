
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.StringTokenizer;

/*
1. Taking a parameters 'K' from user to define reservoir sampling size
2. Created custom SampleInputFormat and SampleRecordReader to facilitate reservoir sampling
3. At getSplits, defining K/splits.size() to randomly select lines
4. Randomly selecting lines in Record Reader nextKeyValue
5. No random-ing in mapper or on splits has been done
6. Code tested against multiple values of K
 */
public class GrepHadoop {

    public static void main(String[] args) throws Exception {

        System.out.println("This is Varun Ojha");

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("These are required : <in> <out> <keyword>");
            System.exit(2);
        }

        System.out.println("to check if changes went through");

        conf.set("search", otherArgs[2]);

        // Setting K within the configuration so that it can be used in TextInputFormat for reservoir sampling
        conf.set("K",otherArgs[3]);

        Job job = new Job(conf, "grep");

        job.setJarByClass(GrepHadoop.class);
        job.setMapperClass(WordMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(NYUSampleInputFormat.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        FileInputFormat.setInputPaths(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        int result = job.waitForCompletion(true) ? 0 : 1;

        if(result == 0)
        {
            System.out.println("Submission by Varun Ojha, net id : vo383 Reservoir Sampling \n\n");
        }
        System.exit(result);

    }

    public static class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Random r = new Random();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            String search = conf.get("search");



            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            StringTokenizer itr = new StringTokenizer(value.toString());
            Text temp = new Text("");
            int len = 20;
            String[] line = new String[len];
            int cnt = 0;

            while (itr.hasMoreTokens() ) {

                word.set(itr.nextToken().toLowerCase());
                line[cnt] = word.toString();
                cnt++;

                if(cnt == len -1)
                {
                    cnt = 0;
                }
                if( word.toString().equals( search ) ) {
                    temp = new Text("file -> \t" + fileName + ",  line#" + key+"  , line :\t"+ Arrays.toString(line));
                        context.write(temp, one);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                result.set(val.get());
                context.write(key, result);
            }
        }
    }
}

