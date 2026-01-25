import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class GraphValue implements Writable, Cloneable {
    public long count;
    public long win;
    public long loss; // for edges only to revert wins

    public GraphValue() {}

    public GraphValue(long count, long win, long loss) {
        this.count = count;
        this.win = win;
        this.loss = loss;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, count);
        WritableUtils.writeVLong(out, win);
        WritableUtils.writeVLong(out, loss);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        count = WritableUtils.readVLong(in);
        win = WritableUtils.readVLong(in);
        loss = WritableUtils.readVLong(in);
    }

    @Override
    public String toString() {
        return count + "," + win + "," + loss;
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