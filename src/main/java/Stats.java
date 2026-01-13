import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Stats extends Configured implements Tool {

    public static class StatsMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private final Map<String, Long> archetypeCounts = new HashMap<>();
        private long totalCount = 0L;
        private int archetypeSize = 2; // taille d'archétype utilisée dans Graph (par défaut 2)
        private double totalGames = 0.0; // estimation du nombre total de parties

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            // même paramètre que dans Graph (si passé), sinon valeur par défaut 2
            archetypeSize = conf.getInt("ArchetypeSize", 2);
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) {
                return;
            }

            FileSystem fs = FileSystem.get(conf);
            for (URI uri : cacheFiles) {
                Path path = new Path(uri.getPath());
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty()) {
                            continue;
                        }
                        String[] parts = line.split(",");
                        if (parts.length < 3) {
                            continue;
                        }
                        String archetype = parts[0];
                        long count;
                        try {
                            count = Long.parseLong(parts[1]);
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        archetypeCounts.put(archetype, count);
                        totalCount += count;
                    }
                }
            }

            // Dans Graph, chaque partie contribue 2 * C(8, archetypeSize) occurrences de nœuds.
            // Donc: totalCount = totalGames * 2 * C(8, archetypeSize)
            //  => totalGames = totalCount / (2 * C(8, archetypeSize))
            long comb = combinations(8, archetypeSize);
            if (totalCount > 0L && comb > 0L) {
                totalGames = (double) totalCount / (2.0 * (double) comb);
            }
        }

        // Calcul de C(n, k) pour n petit (ici n = 8)
        private long combinations(int n, int k) {
            if (k < 0 || k > n) {
                return 0L;
            }
            if (k == 0 || k == n) {
                return 1L;
            }
            k = Math.min(k, n - k);
            long result = 1L;
            for (int i = 1; i <= k; i++) {
                result = result * (n - (k - i)) / i;
            }
            return result;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            // edges lines are: source,target,count,wins
            String[] parts = line.split(",");
            if (parts.length < 4) {
                return;
            }

            String source = parts[0];
            String target = parts[1];

            long count;
            long wins;
            try {
                count = Long.parseLong(parts[2]);
                wins = Long.parseLong(parts[3]);
            } catch (NumberFormatException e) {
                return;
            }

            Long sourceCountObj = archetypeCounts.get(source);
            Long targetCountObj = archetypeCounts.get(target);
            if (sourceCountObj == null || targetCountObj == null || totalCount <= 0L || totalGames <= 0.0) {
                return;
            }

            long sourceCount = sourceCountObj;
            long targetCount = targetCountObj;

            // Prévision basée sur l'hypothèse d'indépendance:
            // E[count(source,target)] = count(source) * count(target) / totalGames
            double prevision = ((double) sourceCount * (double) targetCount) / totalGames;

            StringBuilder out = new StringBuilder();
            // Archetype source ; Archetype target ; count ; win ; count source,count target ; prevision
            out.append(source).append(';')
               .append(target).append(';')
               .append(count).append(';')
               .append(wins).append(';')
               .append(sourceCount).append(',').append(targetCount).append(';')
               .append(prevision);

            context.write(NullWritable.get(), new Text(out.toString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        if (args.length < 2) {
            System.out.println("bad arguments, waiting for 2 arguments [graphOutputURI] [statsOutputURI]");
            return -1;
        }

        Path graphOutputPath = new Path(args[0]);
        Path statsOutputPath = new Path(args[1]);

        Job job = Job.getInstance(conf, "Stats data");
        job.setJarByClass(Stats.class);

        job.setMapperClass(StatsMapper.class);
        job.setNumReduceTasks(0); // map-only job

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Input: only edges files from Graph output (edges-*)
        FileInputFormat.addInputPath(job, new Path(graphOutputPath, "edges*"));

        // Add nodes files (nodes-*) to distributed cache
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] statuses = fs.listStatus(graphOutputPath);
        if (statuses != null) {
            for (FileStatus status : statuses) {
                String name = status.getPath().getName();
                if (name.startsWith("nodes")) {
                    job.addCacheFile(status.getPath().toUri());
                }
            }
        }

        FileOutputFormat.setOutputPath(job, statsOutputPath);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis();
        int exitCode = ToolRunner.run(new Stats(), args);
        long endTime = System.currentTimeMillis();
        long durationMs = endTime - startTime;
        System.out.println("Temps d'exécution des stats : " + durationMs + " ms (" + (durationMs / 1000.0) + " s)");
        System.exit(exitCode);
    }
}
