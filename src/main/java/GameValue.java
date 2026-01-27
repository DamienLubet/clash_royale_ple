import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class GameValue implements Writable, Cloneable, Comparable<GameValue> {
    long timestamp;
    String json;

    public GameValue() {}
    
    public GameValue(long timestamp, String json) {
        this.timestamp = timestamp;
        this.json = json;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeUTF(json);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        timestamp = in.readLong();
        json = in.readUTF();
    }

    @Override
    public String toString() {
        return timestamp + "," + json;
    }

    @Override
    public GameValue clone() {
        try {
            return (GameValue) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Clone not supported", e);
        }
    }

    @Override
    public int compareTo(GameValue other) {
        return Long.compare(this.timestamp, other.timestamp);
    }
}