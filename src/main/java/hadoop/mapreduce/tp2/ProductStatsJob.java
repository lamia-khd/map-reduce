package hadoop.mapreduce.tp2;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Exercice 2 - Statistiques par categorie de produit (sans combiner)
 * Entree : ligne d'achat (TSV a 6 champs)
 * Mapper : (Text product, DoubleWritable cost)
 * Reducer : (Text product, Text "count\tmin\tmax\ttotal\tavg")
 */
public class ProductStatsJob {

    public static class ProductStatsMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text product = new Text();
        private final DoubleWritable cost = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length != 6) {
                context.getCounter("QUALITY", "MALFORMED_LINES").increment(1);
                return;
            }

            try {
                String category = fields[3].trim();
                double amount = Double.parseDouble(fields[4].trim());
                product.set(category);
                cost.set(amount);
                context.write(product, cost);
            } catch (NumberFormatException e) {
                context.getCounter("QUALITY", "MALFORMED_COST").increment(1);
            }
        }
    }

    public static class ProductStatsReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private final Text out = new Text();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            double sum = 0.0;
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;

            for (DoubleWritable v : values) {
                double val = v.get();
                count++;
                sum += val;
                if (val < min) min = val;
                if (val > max) max = val;
            }

            if (count > 0) {
                double avg = sum / count;
                // Format: count \t min \t max \t total \t avg
                String stats = String.format(Locale.US, "%d\t%.2f\t%.2f\t%.2f\t%.2f", count, min, max, sum, avg);
                out.set(stats);
                context.write(key, out);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ProductStatsJob <input_path> <output_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Product Statistics (Without Combiner)");

        job.setJarByClass(ProductStatsJob.class);
        job.setMapperClass(ProductStatsMapper.class);
        // Note: PAS de combiner dans cette premiere version car la moyenne n'est ni associative ni commutative
        job.setReducerClass(ProductStatsReducer.class);

        // Types de sortie Mapper (donnees intermediaires)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // Types de sortie Reducer (donnees finales)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

