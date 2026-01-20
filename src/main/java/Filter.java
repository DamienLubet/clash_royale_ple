import java.io.IOException;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Filter extends Configured implements Tool {

    public static boolean deckIsValid(String deck) {
        // A valid deck contains exactly 8 cards
        // A card is 2 digits
        return deck != null && deck.length() == 16;  
    }

    public static class FilterMapper extends Mapper<LongWritable, Text, Game, Text> {
        JsonParser parser = new JsonParser();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String json = value.toString().trim();
            JsonObject jsonObject;
            try {
                jsonObject = parser.parse(json).getAsJsonObject();
            } catch (Exception e) {
                context.getCounter("Filtering Stats", "Invalid JSON").increment(1);
                return;
            }

            String date = jsonObject.get("date").getAsString();
            int round = jsonObject.get("round").getAsInt();
            JsonArray players = jsonObject.getAsJsonArray("players");
        
            JsonObject player1 = players.get(0).getAsJsonObject();
            String player1ID = player1.get("utag").getAsString();
            String player1Deck = player1.get("deck").getAsString();

            JsonObject player2 = players.get(1).getAsJsonObject();
            String player2ID = player2.get("utag").getAsString();
            String player2Deck = player2.get("deck").getAsString();

            if (!deckIsValid(player1Deck) || !deckIsValid(player2Deck)){
                context.getCounter("Filtering Stats", "Invalid Deck").increment(1);
                return;
            }
                
            String a = player1ID.compareTo(player2ID) < 0 ? player1ID : player2ID;
            String b = player1ID.compareTo(player2ID) < 0 ? player2ID : player1ID;
            Game game = new Game(date, a, b, round);
            context.write(game, new Text(json));
        }
    }
    
    public static class FilterReducer extends Reducer<Game, Text, NullWritable, Text> {
        @Override
        protected void reduce(Game key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(NullWritable.get(), values.iterator().next());
        }
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Clean data");
        
        job.setJarByClass(Filter.class);
        
        job.setMapOutputKeyClass(Game.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
            int numReducers = Integer.parseInt(args[2]);
            job.setNumReduceTasks(numReducers);
		} 
		catch (Exception e) {
			System.out.println(" bad arguments, waiting for 2 arguments [inputURI] [outputURI]");
			return -1;
		}
        job.setMapperClass(FilterMapper.class);
        job.setReducerClass(FilterReducer.class);

		return job.waitForCompletion(true) ? 0 : 1;
    }
    
        public static void main(String args[]) throws Exception {
            long startTime = System.currentTimeMillis();
            int exitCode = ToolRunner.run(new Filter(), args);
            long endTime = System.currentTimeMillis();
            long durationMs = endTime - startTime;
            System.out.println("Temps d'exécution du filtre : " + durationMs + " ms (" + (durationMs / 1000.0) + " s)");
            System.exit(exitCode);
        }
}
