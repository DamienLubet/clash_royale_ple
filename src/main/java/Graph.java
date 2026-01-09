import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Graph extends Configured implements Tool {
    
    
    public static String[] getArchetype(String deck, int archetypeSize) {
        if (deck == null || deck.length() < archetypeSize * 2) {
            return new String[0];
        }

        List<String> cards = new ArrayList<>();
        for (int i = 0; i < deck.length(); i += 2) {
            if (i + 2 <= deck.length()) {
                cards.add(deck.substring(i, i + 2));
            }
        }

        Collections.sort(cards);

        List<String> results = new ArrayList<>();
        StringBuilder sb = new StringBuilder(archetypeSize * 2);
        combine(cards, archetypeSize, 0, sb, results);

        return results.toArray(new String[0]);
    }

    private static void combine(List<String> cards, int k, int start, StringBuilder current, List<String> results) {
        if (k == 0) {
            results.add(current.toString());
            return;
        }

        for (int i = start; i <= cards.size() - k; i++) {
            int lenBefore = current.length();
            current.append(cards.get(i));
            combine(cards, k - 1, i + 1, current, results);
            current.setLength(lenBefore);
        }
    }
    

    public static class GraphMapper extends Mapper<LongWritable, Text, Text, Text> {

        JsonParser parser = new JsonParser();
        public int archetypeSize;
        public Text outputKey = new Text();
        public Text outputValue = new Text();

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            archetypeSize = conf.getInt("ArchetypeSize", 8);
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String json = value.toString();
            JsonObject jsonObject;
            try {
                jsonObject = parser.parse(json).getAsJsonObject();
            } catch (Exception e) {
                return; // Should not happen
            }

            JsonArray players = jsonObject.getAsJsonArray("players");
            JsonObject player1 = players.get(0).getAsJsonObject();
            String player1Deck = player1.get("deck").getAsString();

            JsonObject player2 = players.get(1).getAsJsonObject();
            String player2Deck = player2.get("deck").getAsString();

            int winner = jsonObject.get("winner").getAsInt();
            String[] archetype1 = getArchetype(player1Deck, archetypeSize);
            String[] archetype2 = getArchetype(player2Deck, archetypeSize);
            context.getCounter("Graph", "TotalGames").increment(1);

            int gameCount = 1;
            int winCount1 = (winner == 0) ? 1 : 0;
            int winCount2 = (winner == 1) ? 1 : 0;

            for (int i = 0; i < archetype1.length; i++) {
                // Emit node data
                outputKey.set(archetype1[i]);
                outputValue.set(gameCount + "," + winCount1);
                context.write(outputKey, outputValue);

                outputKey.set(archetype2[i]);
                outputValue.set(gameCount + "," + winCount2);
                context.write(outputKey, outputValue);

                // Emit edge data
                for (int j = 0; j < archetype1.length; j++) {
                    outputKey.set(archetype1[i] + "," + archetype2[j]);
                    outputValue.set(gameCount + "," + winCount1);
                    context.write(outputKey, outputValue);

                    outputKey.set(archetype2[j] + "," + archetype1[i]);
                    outputValue.set(gameCount + "," + winCount2);
                    context.write(outputKey, outputValue);
                }
            }
        }
        
    }

    public static class GraphCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int totalGames = 0;
            int totalWins = 0;
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                totalGames += Integer.parseInt(parts[0]);
                totalWins += Integer.parseInt(parts[1]);
            }
            context.write(key, new Text(totalGames + "," + totalWins));
        }
    }

    public static class GraphReducer extends Reducer<Text, Text, NullWritable, Text> {
        MultipleOutputs<NullWritable, Text> multipleOutputs;
        int archetypeSize;

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            archetypeSize = conf.getInt("ArchetypeSize", 8);
            multipleOutputs = new MultipleOutputs<>(context);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int totalGames = 0;
            int totalWins = 0;
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                totalGames += Integer.parseInt(parts[0]);
                totalWins += Integer.parseInt(parts[1]);
            }

            String outValue = key.toString() + "," + totalGames + "," + totalWins;
            context.getCounter("Graph", "UniqueKeys").increment(1);
            context.getCounter("Graph", "TotalGamesAggregated").increment(totalGames);
            context.getCounter("Graph", "TotalWinsAggregated").increment(totalWins);
            if (key.toString().contains(",")) {
                context.getCounter("Graph", "Edges").increment(1);
                multipleOutputs.write("edges", NullWritable.get(), new Text(outValue));
            } else {
                context.getCounter("Graph", "Nodes").increment(1);
                multipleOutputs.write("nodes", NullWritable.get(), new Text(outValue));
            }
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Graph data");
        
        job.setJarByClass(Graph.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            int archetypeSize = Integer.parseInt(args[2]);
            System.out.println("Archetype Size: " + archetypeSize);
            if (archetypeSize <= 0 || archetypeSize > 8) {
                System.out.println("ArchetypeSize must be between 1 and 8");
                return -1;
            }
            job.getConfiguration().setInt("ArchetypeSize", archetypeSize);
            conf.set("mapreduce.task.io.sort.mb", "512");
            conf.set("mapreduce.map.sort.spill.percent", "0.90");
		} 
		catch (Exception e) {
			System.out.println(" bad arguments, waiting for 3 arguments [inputURI] [outputURI] [ArchetypeSize]");
			return -1;
		}
        job.setMapperClass(GraphMapper.class);
        job.setReducerClass(GraphReducer.class);
        job.setCombinerClass(GraphCombiner.class);

        // FileSystem fs = FileSystem.get(conf);
        // ContentSummary cs = fs.getContentSummary(new Path(args[0]));
        //long inputSize = cs.getLength();

        //int numReducers = (int) Math.max(1, inputSize / (1024 * 1024 * 1024));
        job.setNumReduceTasks(1);
        MultipleOutputs.addNamedOutput(job, "nodes", TextOutputFormat.class, NullWritable.class, Text.class);
        MultipleOutputs.addNamedOutput(job, "edges", TextOutputFormat.class, NullWritable.class, Text.class);
		return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String args[]) throws Exception {
        long startTime = System.currentTimeMillis();
            int exitCode = ToolRunner.run(new Graph(), args);
            long endTime = System.currentTimeMillis();
            long durationMs = endTime - startTime;
            System.out.println("Temps d'exécution du filtre : " + durationMs + " ms (" + (durationMs / 1000.0) + " s)");
            System.exit(exitCode);
    }
}
