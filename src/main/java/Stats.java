import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Stats extends Configured implements Tool {

    public static class NodeMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            
            String[] parts = line.split(",");
            if (parts.length < 3) return;
            
            String archetype = parts[0];
            String count = parts[1];
            String win = parts[2];

            context.write(NullWritable.get(), new Text("NODE;" + archetype + ";" + count + ";" + win));
        }
    }

    public static class EdgeMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            
            String[] parts = line.split(",");
            if (parts.length < 4) return;
            
            String archetypeSource = parts[0];
            String archetypeTarget = parts[1];
            String count = parts[2];
            String win = parts[3];

            context.write(NullWritable.get(), new Text("EDGE;" + archetypeSource + ";" + archetypeTarget + ";" + count + ";" + win));
        }
    }

    public static class StatsReducer extends Reducer<NullWritable, Text, NullWritable, Text> {
        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            int archetype = 0;
            List<String> edgesList = new ArrayList<>();
            Map<String, Long> nodeList = new HashMap<>();        

            for (Text val : values) {
                String s = val.toString();
                
                if (s.startsWith("NODE;")) {
                    String[] parts = s.split(";");
                    String arch = parts[1];
                    long count = Long.parseLong(parts[2]);
                    
                    nodeList.put(arch, count);
                    archetype += count;
                } 
                else if (s.startsWith("EDGE;")) {
                    edgesList.add(s.substring(5));
                }
            }

            for (String edge : edgesList) {
                String[] parts = edge.split(";");
                String source = parts[0];
                String target = parts[1];
                String edgeCount = parts[2];
                String edgeWin = parts[3];

                Long sourceCount = nodeList.getOrDefault(source, 0L);
                Long targetCount = nodeList.getOrDefault(target, 0L);


                if (sourceCount != null && targetCount != null && archetype > 0) {
                    double prevision = (double) (sourceCount * targetCount) / archetype;

                    StringBuilder sb = new StringBuilder();
                    sb.append(source).append(";");
                    sb.append(target).append(";");
                    sb.append(edgeCount).append(";");
                    sb.append(edgeWin).append(";");
                    sb.append(sourceCount).append(";");
                    sb.append(targetCount).append(";");
                    sb.append(String.format("%.2f", prevision));

                    context.write(NullWritable.get(), new Text(sb.toString()));
                }
            }
        }
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "Stats data");
        job.setJarByClass(Stats.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        try {
			MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, NodeMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, EdgeMapper.class);
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
		} 
        catch (Exception e) {
            System.out.println(" bad arguments, waiting for 3 arguments [NodeInputURI] [EdgeInputURI] [statsOutputURI]");
            return -1;
        }

        job.setReducerClass(StatsReducer.class);
        job.setNumReduceTasks(1);
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
