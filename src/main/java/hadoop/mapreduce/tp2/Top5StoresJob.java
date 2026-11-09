package hadoop.mapreduce.tp2;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
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
 * Exercice 3 - Top 5 des magasins par ventes totales (Conception A)
 * 
 * - Mapper : extrait le magasin (fields[2]) et le cout (fields[4]),
 *            emet (Text store, DoubleWritable cost)
 * - Combiner (SumCombiner) : additionne localement les couts par magasin
 *            emet (Text store, DoubleWritable sommePartielle)
 *            NOTE: le combiner ne fait PAS le filtrage Top-5 !
 * - Reducer unique (Top5Reducer) :
 *            - Calcule le total pour chaque magasin dans reduce()
 *            - Met a jour un TreeMap<Double, String> limite a 5 elements
 *            - Dans cleanup(), ecrit les 5 plus grands magasins tries par ordre decroissant
 */
public class Top5StoresJob {

    public static class Top5Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final Text store = new Text();
        private final DoubleWritable cost = new DoubleWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length != 6) {
                context.getCounter("QUALITY", "MALFORMED_LINES").increment(1);
                return;
            }

            try {
                String storeName = fields[2].trim();
                double amount = Double.parseDouble(fields[4].trim());
                store.set(storeName);
                cost.set(amount);
                context.write(store, cost);
            } catch (NumberFormatException e) {
                context.getCounter("QUALITY", "MALFORMED_COST").increment(1);
            }
        }
    }

    /**
     * Combiner : additionne les montants par magasin sur chaque nœud mapper.
     * Le combiner ne doit PAS etre la meme classe que le Reducer car le filtrage
     * Top-N n'est valide qu'au niveau global dans le Reducer final.
     */
    public static class SumCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private final DoubleWritable partialSum = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            partialSum.set(sum);
            context.write(key, partialSum);
        }
    }

    /**
     * Reducer unique (job.setNumReduceTasks(1)) avec TreeMap et methode cleanup().
     */
    public static class Top5Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private static final int N = 5;
        // TreeMap trie par cle naturelle croissante (le montant total)
        private final TreeMap<Double, String> top = new TreeMap<>();
        private final Text outStore = new Text();
        private final DoubleWritable outTotal = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double total = 0.0;
            for (DoubleWritable val : values) {
                total += val.get();
            }

            top.put(total, key.toString());
            // Si la taille depasse N (5), on retire le plus petit (premiere cle)
            if (top.size() > N) {
                top.remove(top.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Parcourir par ordre decroissant pour avoir du 1er au 5eme
            for (Map.Entry<Double, String> entry : top.descendingMap().entrySet()) {
                outStore.set(entry.getValue());
                outTotal.set(entry.getKey());
                context.write(outStore, outTotal);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: Top5StoresJob <input_path> <output_path>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        if (conf.get("mapreduce.framework.name") == null) {
            conf.set("mapreduce.framework.name", "local");
        }
        if (conf.get("fs.defaultFS") == null) {
            conf.set("fs.defaultFS", "file:///");
        }
        Job job = Job.getInstance(conf, "Top 5 Stores By Total Sales (Conception A)");

        job.setJarByClass(Top5StoresJob.class);
        job.setMapperClass(Top5Mapper.class);
        
        // Utilisation du Combiner specifique pour additionner
        job.setCombinerClass(SumCombiner.class);
        
        // Reducer avec TreeMap dans cleanup()
        job.setReducerClass(Top5Reducer.class);

        // OBLIGATOIRE pour un classement global : 1 seul Reducer !
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(conf).delete(outputPath, true);
        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

