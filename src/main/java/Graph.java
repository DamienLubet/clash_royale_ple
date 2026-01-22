import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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
    public static class GraphMapper extends Mapper<LongWritable, Text, Text, GraphValue> {

        JsonParser parser = new JsonParser();
        public int archetypeSize;

        // Reusable objects
        public Text outputKey;
        public GraphValue outputValue = new GraphValue();
        public StringBuilder sb;
        public StringBuilder edgeBuilder = new StringBuilder();
        public String[] cards;
        public List<String> archetype1;
        public List<String> archetype2;

        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            archetypeSize = conf.getInt("ArchetypeSize", 8);
            sb = new StringBuilder(archetypeSize * 2);
            edgeBuilder = new StringBuilder();
            cards = new String[8];
            outputKey = new Text();
            archetype1 = new ArrayList<>(70);
            archetype2 = new ArrayList<>(70);
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
            getArchetype(player1Deck, archetypeSize, cards, sb, archetype1);
            getArchetype(player2Deck, archetypeSize, cards, sb, archetype2);
            context.getCounter("Graph", "TotalGames").increment(1);

            long gameCount = 1;
            long winCount1 = (winner == 0) ? 1 : 0;
            long winCount2 = (winner == 1) ? 1 : 0;
            
            int size1 = archetype1.size();
            int size2 = archetype2.size();

            for (int i = 0; i < size1; i++) {
                outputKey.set(archetype1.get(i));
                outputValue.count = gameCount;
                outputValue.win = winCount1;
                context.write(outputKey, outputValue);
            }

            for (int i = 0; i < size2; i++) {
                outputKey.set(archetype2.get(i));
                outputValue.count = gameCount;
                outputValue.win = winCount2;
                context.write(outputKey, outputValue);
            }

           for(int i = 0; i < size1; i++) {
                String arch1 = archetype1.get(i);
                for (int j = 0; j < size2; j++) {
                    String arch2 = archetype2.get(j);

                    edgeBuilder.setLength(0);
                    edgeBuilder.append(arch1).append(',').append(arch2);
                    outputKey.set(edgeBuilder.toString());
                    outputValue.count = gameCount;
                    outputValue.win = (winner == 0) ? 1 : 0;
                    context.write(outputKey, outputValue);

                    // Edge J2 -> J1
                    edgeBuilder.setLength(0);
                    edgeBuilder.append(arch2).append(',').append(arch1);
                    outputKey.set(edgeBuilder.toString());
                    outputValue.count = gameCount;
                    outputValue.win = (winner == 1) ? 1 : 0;
                    context.write(outputKey, outputValue);
                }
            }
        }
        
        public static void getArchetype(String deck, int archetypeSize, String[] cards, StringBuilder sb, List<String> results) {
            if (deck == null || deck.length() < archetypeSize * 2) {
                return;
            }

            sb.setLength(0);
            results.clear();

            for (int i = 0; i < deck.length(); i += 2) {
                if (i + 2 <= deck.length()) {
                    String card = deck.substring(i, i + 2);
                    cards[i / 2] = card;
                }
            }

            Arrays.sort(cards);
            combine(cards, archetypeSize, 0, sb, results);
        }

        private static void combine( String[] cards, int k, int start, StringBuilder current, List<String> results) {
            if (k == 0) {
                results.add(current.toString());
                return;
            }

            for (int i = start; i <= cards.length - k; i++) {
                int lenBefore = current.length();
                current.append(cards[i]);
                combine(cards, k - 1, i + 1, current, results);
                current.setLength(lenBefore);
            }
        }
    }

    public static class GraphCombiner extends Reducer<Text, GraphValue, Text, GraphValue> {
        GraphValue outputValue = new GraphValue();
        @Override
        protected void reduce(Text key, Iterable<GraphValue> values, Context context)
                throws IOException, InterruptedException {
            long totalGames = 0;
            long totalWins = 0;
            for (GraphValue val : values) {
                totalGames += val.count;
                totalWins += val.win;
            }
            outputValue.count = totalGames;
            outputValue.win = totalWins;
            context.write(key, outputValue);
        }
    }

    public static class GraphReducer extends Reducer<Text, GraphValue, NullWritable, Text> {
        MultipleOutputs<NullWritable, Text> multipleOutputs;
        

        public void setup(Context context) {
            multipleOutputs = new MultipleOutputs<>(context);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

        @Override
        protected void reduce(Text key, Iterable<GraphValue> values, Context context)
                throws IOException, InterruptedException {
            long totalGames = 0;
            long totalWins = 0;
            for (GraphValue val : values) {
                totalGames += val.count;
                totalWins += val.win;
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

        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        conf.set("mapreduce.task.io.sort.mb", "512");
        conf.set("mapreduce.map.memory.mb", "2048");
        conf.set("mapreduce.map.sort.spill.percent", "0.90");
        conf.set("mapreduce.map.java.opts", "-Xmx1638m -XX:+UseG1GC -XX:+UseStringDeduplication");

        Job job = Job.getInstance(conf, "Graph data");
        
        job.setJarByClass(Graph.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(GraphValue.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            int archetypeSize = Integer.parseInt(args[2]);
            int numReducer = Integer.parseInt(args[3]);
            if (numReducer <= 0) {
                numReducer = 1;
            }
            job.setNumReduceTasks(numReducer);
            System.out.println("Archetype Size: " + archetypeSize);
            if (archetypeSize <= 0 || archetypeSize > 8) {
                System.out.println("ArchetypeSize must be between 1 and 8");
                return -1;
            }
            job.getConfiguration().setInt("ArchetypeSize", archetypeSize);
		} 
		catch (Exception e) {
			System.out.println(" bad arguments, waiting for 4 arguments [inputURI] [outputURI] [ArchetypeSize] [numReducer]");
			return -1;
		}
        job.setMapperClass(GraphMapper.class);
        job.setReducerClass(GraphReducer.class);
        job.setCombinerClass(GraphCombiner.class);


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
