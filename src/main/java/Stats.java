import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Locale;
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

    public static class MapSideJoinMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

        private Map<String, Long> nodesMap = new HashMap<>();
        private long totalArchetypes = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalArchetypes = context.getConfiguration().getLong("TOTAL_ARCHETYPES", 1);

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI fileUri : cacheFiles) {
                    Path p = new Path(fileUri.getPath());
                    String filename = p.getName(); 

                    loadFile(filename);
                }
            }
        }

        private void loadFile(String filename) {
            try (BufferedReader reader = new BufferedReader(new FileReader(filename))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        try {
                            String archetype = parts[0];
                            long count = Long.parseLong(parts[1]);
                            nodesMap.put(archetype, count);
                        } catch (NumberFormatException e) {
                        }
                    }
                }
            } catch (IOException e) {
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 4) return;

            String source = parts[0];
            String target = parts[1];
            String edgeCount = parts[2];
            String edgeWin = parts[3];

            Long sourceCount = nodesMap.get(source);
            Long targetCount = nodesMap.get(target);

            if (sourceCount != null && targetCount != null && totalArchetypes > 0) {
                
                double prevision = ((double) sourceCount * targetCount) / totalArchetypes;

                StringBuilder sb = new StringBuilder();
                sb.append(source).append(";");
                sb.append(target).append(";");
                sb.append(edgeCount).append(";");
                sb.append(edgeWin).append(";");
                sb.append(sourceCount).append(";");
                sb.append(targetCount).append(";");
                sb.append(String.format(Locale.US, "%.2f", prevision));

                context.write(NullWritable.get(), new Text(sb.toString()));
            }
        }


    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        
        if (args.length < 3) {
            System.err.println("Usage: StatsMapSide <NodeInput> <EdgeInput> <Output>");
            return -1;
        }

        Path nodePath = new Path(args[0]);
        Path edgePath = new Path(args[1]);
        Path outputPath = new Path(args[2]);

        FileSystem fs = FileSystem.get(conf);
        
        FileStatus[] nodeFiles = fs.globStatus(nodePath);
        if (nodeFiles == null || nodeFiles.length == 0) {
            System.err.println("No node files found at " + nodePath.toString());
            return -1;
        }
        
        long totalSum = 0;
        for (FileStatus status : nodeFiles) {
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        try {
                            totalSum += Long.parseLong(parts[1]);
                        } catch (NumberFormatException e) {  }
                    }
                }
            }
        }
        
        conf.setLong("TOTAL_ARCHETYPES", totalSum);


        Job job = Job.getInstance(conf, "Stats Map-Side Join");
        job.setJarByClass(Stats.class);

        job.setMapperClass(MapSideJoinMapper.class);
        
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, edgePath);
        FileOutputFormat.setOutputPath(job, outputPath);

        for (FileStatus status : nodeFiles) {
            job.addCacheFile(status.getPath().toUri());
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        int exitCode = ToolRunner.run(new Stats(), args);
        long end = System.currentTimeMillis();
        System.out.println("Temps d'execution : " + (end - start) + " ms");
        System.exit(exitCode);
    }
}