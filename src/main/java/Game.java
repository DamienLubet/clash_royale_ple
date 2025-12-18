import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;

import org.apache.hadoop.io.WritableComparable;


public class Game implements Cloneable, WritableComparable<Game> {
    String date;
    String playerID;
    String opponentID;
    int round;

    public Game() {}

    public Game(String date, String playerID, String opponentID, int round) {
        this.date = date;
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

    @Override
public int compareTo(Game other) {

    // --- 1. Comparaison des joueurs (ordre indépendant) ---
    String thisP1 = playerID.compareTo(opponentID) <= 0 ? playerID : opponentID;
    String thisP2 = playerID.compareTo(opponentID) <= 0 ? opponentID : playerID;

    String otherP1 = other.playerID.compareTo(other.opponentID) <= 0
            ? other.playerID : other.opponentID;
    String otherP2 = other.playerID.compareTo(other.opponentID) <= 0
            ? other.opponentID : other.playerID;

    int cmp = thisP1.compareTo(otherP1);
    if (cmp != 0) return cmp;

    cmp = thisP2.compareTo(otherP2);
    if (cmp != 0) return cmp;

    cmp = Integer.compare(this.round, other.round);
    if (cmp != 0) return cmp;

    long thisTime = Instant.parse(this.date).getEpochSecond();
    long otherTime = Instant.parse(other.date).getEpochSecond();

    long diff = Math.abs(thisTime - otherTime);

    if (diff <= 10) {
        return 0; 
    }

    return Long.compare(thisTime, otherTime);
}
}
