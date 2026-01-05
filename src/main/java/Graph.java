import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
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
    
    public static String getArchetype(String deck, int archetypeSize) {
        // An archetype is defined as the first 'archetypeSize' cards of the deck
        return deck.substring(0, archetypeSize * 2);
    }

    public static class GraphMapper extends Mapper<LongWritable, Text, Text, Text> {

        JsonParser parser = new JsonParser();
        public int archetypeSize;
        public Map<EdgeKey, int[]> edgeMap = new HashMap<>();
        public Map<String, int[]> nodeMap = new HashMap<>();

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
            String archetype1 = getArchetype(player1Deck, archetypeSize);
            String archetype2 = getArchetype(player2Deck, archetypeSize);

            updateStats(archetype1, true, winner == 0);
            updateStats(archetype2, true, winner == 1);

            // create edge key
            EdgeKey edge = new EdgeKey(archetype1, archetype2);
            EdgeKey reverseEdge = new EdgeKey(archetype2, archetype1);
            int[] edgeValue = new int[2];
            edgeValue[0] = 1;
            if (winner == 0)
                edgeValue[1] = 1;
            else
                edgeValue[1] = 0;

            edgeMap.put(edge, edgeValue);
            edgeValue[1] = winner == 1 ? 1 : 0;
            edgeMap.put(reverseEdge, edgeValue);
        }

        private void updateStats(String key, boolean played, boolean won) {
            int[] current = nodeMap.get(key);
            if (current == null) {
                current = new int[]{0, 0};
                nodeMap.put(key, current);
            }
            if (played) current[0]++;
            if (won) current[1]++;
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Emit nodes
            for (Map.Entry<String, int[]> entry : nodeMap.entrySet()) {
                String archetype = entry.getKey();
                int[] values = entry.getValue();
                String outValue = values[0] + "," + values[1];
                context.write(new Text(archetype), new Text(outValue));
            }

            // Emit edges
            for (Map.Entry<EdgeKey, int[]> entry : edgeMap.entrySet()) {
                EdgeKey edge = entry.getKey();
                int[] values = entry.getValue();
                String key = edge.source + "," + edge.target;
                String outValue = values[0] + "," + values[1];
                context.write(new Text(key), new Text(outValue));
            }
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
            if (key.toString().split(",").length == 2) {
                for (Text val : values) {
                    // Edge
                    String[] parts = val.toString().split(",");
                    int games = Integer.parseInt(parts[0]);
                    int wins = Integer.parseInt(parts[1]);
                    totalGames += games;
                    totalWins += wins;
                    continue;
                }
                String outValue = key.toString() + "," + totalGames + "," + totalWins;
                multipleOutputs.write("edges", NullWritable.get(), new Text(outValue));
                return;
            }
            for (Text val : values) {
                // Node
                String[] parts = val.toString().split(",");
                int games = Integer.parseInt(parts[0]);
                int wins = Integer.parseInt(parts[1]);
                totalGames += games;
                totalWins += wins;
            }
            String outValue = key.toString() + "," + totalGames + "," + totalWins;
            multipleOutputs.write("nodes", NullWritable.get(), new Text(outValue));
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
		} 
		catch (Exception e) {
			System.out.println(" bad arguments, waiting for 3 arguments [inputURI] [outputURI] [ArchetypeSize]");
			return -1;
		}
        job.setMapperClass(GraphMapper.class);
        job.setReducerClass(GraphReducer.class);

        FileSystem fs = FileSystem.get(conf);
        ContentSummary cs = fs.getContentSummary(new Path(args[0]));
        long inputSize = cs.getLength();

        int numReducers = (int) Math.max(1, inputSize / (1024 * 1024 * 1024));
        System.out.println("Number of reducers: " + numReducers);
        job.setNumReduceTasks(numReducers);
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
