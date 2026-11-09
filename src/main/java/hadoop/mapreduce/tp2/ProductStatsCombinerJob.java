package hadoop.mapreduce.tp2;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Exercice 2 (Bonus) - Statistiques par categorie de produit AVEC Combiner.
 * 
 * Strategie Combiner :
 * Pour calculer la moyenne avec un Combiner, la valeur intermediaire doit etre
 * un tuple "count,sum,min,max" car (count, sum, min, max) est une structure
 * qui admet un monoide associatif et commutatif (on peut sommer les counts et sums,
 * et prendre min(min) et max(max)).
 * 
 * - Mapper : emet (category, "1,cost,cost,cost")
 * - Combiner : agrege les tuples et emet (category, "count,sum,min,max")
 * - Reducer : agrege les tuples et calcule la moyenne finale : (category, "count\tmin\tmax\tsum\tavg")
 */
public class ProductStatsCombinerJob {

    public static class StatsMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text product = new Text();
        private final Text statsValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length != 6) {
                context.getCounter("QUALITY", "MALFORMED_LINES").increment(1);
                return;
            }

            try {
                String category = fields[3].trim();
                double cost = Double.parseDouble(fields[4].trim());
                product.set(category);
                // "1,cost,cost,cost"
                statsValue.set(String.format(Locale.US, "1,%.2f,%.2f,%.2f", cost, cost, cost));
                context.write(product, statsValue);
            } catch (NumberFormatException e) {
                context.getCounter("QUALITY", "MALFORMED_COST").increment(1);
            }
        }
    }

    public static class StatsCombiner extends Reducer<Text, Text, Text, Text> {
        private final Text result = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long totalCount = 0;
            double totalSum = 0.0;
            double globalMin = Double.MAX_VALUE;
            double globalMax = -Double.MAX_VALUE;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length == 4) {
                    long count = Long.parseLong(parts[0]);
                    double sum = Double.parseDouble(parts[1]);
                    double min = Double.parseDouble(parts[2]);
                    double max = Double.parseDouble(parts[3]);

                    totalCount += count;
                    totalSum += sum;
                    if (min < globalMin) globalMin = min;
                    if (max > globalMax) globalMax = max;
                }
            }

            if (totalCount > 0) {
                result.set(String.format(Locale.US, "%d,%.2f,%.2f,%.2f", totalCount, totalSum, globalMin, globalMax));
                context.write(key, result);
            }
        }
    }

    public static class StatsReducer extends Reducer<Text, Text, Text, Text> {
        private final Text out = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            long totalCount = 0;
            double totalSum = 0.0;
            double globalMin = Double.MAX_VALUE;
            double globalMax = -Double.MAX_VALUE;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length == 4) {
                    long count = Long.parseLong(parts[0]);
                    double sum = Double.parseDouble(parts[1]);
                    double min = Double.parseDouble(parts[2]);
                    double max = Double.parseDouble(parts[3]);

                    totalCount += count;
                    totalSum += sum;
                    if (min < globalMin) globalMin = min;
                    if (max > globalMax) globalMax = max;
                }
            }

            if (totalCount > 0) {
                double avg = totalSum / totalCount;
                // Format attendu : produit \t count \t min \t max \t total \t avg
                String stats = String.format(Locale.US, "%d\t%.2f\t%.2f\t%.2f\t%.2f",
                        totalCount, globalMin, globalMax, totalSum, avg);
                out.set(stats);
                context.write(key, out);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ProductStatsCombinerJob <input_path> <output_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        if (conf.get("mapreduce.framework.name") == null) {
            conf.set("mapreduce.framework.name", "local");
        }
        if (conf.get("fs.defaultFS") == null) {
            conf.set("fs.defaultFS", "file:///");
        }
        Job job = Job.getInstance(conf, "Product Statistics (With Combiner)");

        job.setJarByClass(ProductStatsCombinerJob.class);
        job.setMapperClass(StatsMapper.class);
        job.setCombinerClass(StatsCombiner.class);
        job.setReducerClass(StatsReducer.class);

        // Types de sortie Mapper & Combiner (donnees intermediaires)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // Types de sortie Reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

