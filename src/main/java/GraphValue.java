import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hadoop.io.Writable;

public class GraphValue implements Writable, Cloneable {
    public long count;
    public long win;

    public GraphValue() {}

    public GraphValue(long count, long win) {
        this.count = count;
        this.win = win;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, count);
        WritableUtils.writeVLong(out, win);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = WritableUtils.readVLong(in);
        win = WritableUtils.readVLong(in);
    }

    @Override
    public String toString() {
        return count + "," + win;
    }

    @Override
    public GraphValue clone() {
        try {
            return (GraphValue) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Clone not supported", e);
        }
    }
}