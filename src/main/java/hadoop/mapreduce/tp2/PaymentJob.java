package hadoop.mapreduce.tp2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

public class PaymentJob {

    // 1. The Mapper
    public static class PaymentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text payment = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] f = value.toString().split("\t"); // Split by tabulation
            
            // Check for malformed lines
            if (f.length != 6) {
                context.getCounter("QUALITY", "MALFORMED_LINES").increment(1); // Increment error counter
                return;
            }
            
            // f[5] contains the payment method. Trim it and emit (payment, 1)
            payment.set(f[5].trim());
            context.write(payment, ONE);
        }
    }

    // 2. The Reducer
    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // Add up all the 1s for the specific payment method
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result); // Write final count (payment, total)
        }
    }

    // 3. The Driver 
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: PaymentJob <input path> <output path>");
            System.exit(-1);
        }

        if (args.length < 2) {
            System.err.println("Usage: PaymentJob <input_path> <output_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Payment Count");
        
        job.setJarByClass(PaymentJob.class);
        job.setMapperClass(PaymentMapper.class);
        
        // Use the reducer as the combiner as requested
        job.setCombinerClass(SumReducer.class); 
        job.setReducerClass(SumReducer.class);
        
        // Question 4 : Utiliser 3 taches reduce
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
