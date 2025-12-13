import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class Game implements Cloneable, WritableComparable<Game> {
    String date;
    String playerID;
    String opponentID;
    int round;

    public Game() {}

    public Game(String date, String playerID, String opponentID, int round) {
        this.date = date.substring(0, 16); // keep only "YYYY-MM-DD HH:MM"
        this.playerID = playerID;
        this.opponentID = opponentID;
        this.round = round;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(date);
        out.writeUTF(playerID);
        out.writeUTF(opponentID);
        out.writeInt(round);
    }

    public void readFields(DataInput in) throws IOException {
        date = in.readUTF();
        playerID = in.readUTF();
        opponentID = in.readUTF();
        round = in.readInt();
    }

    @Override
    public Game clone() {
        try {
            return (Game) super.clone();
        } catch (CloneNotSupportedException e) {
            System.err.println(e.getStackTrace());
            System.exit(-1);
        }
        return null;
    }

    public int compareTo(Game other) {
        int dateComp = this.date.compareTo(other.date);
        if (dateComp != 0) return dateComp;
        int playerComp = this.playerID.compareTo(other.playerID);
        if (playerComp != 0) return playerComp;
        int opponentComp = this.opponentID.compareTo(other.opponentID);
        if (opponentComp != 0) return opponentComp;
        return Integer.compare(this.round, other.round);
    }
}
