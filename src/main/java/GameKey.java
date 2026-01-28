import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;


public class GameKey implements Cloneable, WritableComparable<GameKey> {
    String playerID;
    String opponentID;
    int round;

    public GameKey() {}

    public GameKey(String playerID, String opponentID, int round) {
        this.playerID = playerID;
        this.opponentID = opponentID;
        this.round = round;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(playerID);
        out.writeUTF(opponentID);
        out.writeInt(round);
    }

    public void readFields(DataInput in) throws IOException {
        playerID = in.readUTF();
        opponentID = in.readUTF();
        round = in.readInt();
    }

    @Override
    public GameKey clone() {
        try {
            return (GameKey) super.clone();
        } catch (CloneNotSupportedException e) {
            System.err.println(e.getStackTrace());
            System.exit(-1);
        }
        return null;
    }

    @Override
    public int compareTo(GameKey other) {
        int cmp = this.playerID.compareTo(other.playerID);
        if (cmp != 0) return cmp;

        cmp = this.opponentID.compareTo(other.opponentID);
        if (cmp != 0) return cmp;

        return Integer.compare(this.round, other.round);
    }

    @Override
    public int hashCode() {
        return playerID.hashCode() * 12 + opponentID.hashCode() * 17 + round;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        GameKey other = (GameKey) obj;
        if (playerID == null) {
            if (other.playerID != null) return false;
        } else if (!playerID.equals(other.playerID)) return false;
        if (opponentID == null) {
            if (other.opponentID != null) return false;
        } else if (!opponentID.equals(other.opponentID)) return false;
        if (round != other.round) return false;
        return true;
    }
}
