import org.apache.hadoop.util.ProgramDriver;

public class ClashRoyale {

	public static void main(String[] args) throws Exception {
		ProgramDriver pgd = new ProgramDriver();
		int exitCode = -1;
		try {
			// Add your classes here
			pgd.addClass("filter", Filter.class, "Filter Clash Royale game data");
			pgd.addClass("graph", Graph.class, "Build Archetype Graph from Clash Royale game data");
			exitCode = pgd.run(args);
		} catch (Throwable e1)  {
			e1.printStackTrace();
		}
		System.exit(exitCode);
	}
	
}