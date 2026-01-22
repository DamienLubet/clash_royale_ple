import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class EdgeKey implements Cloneable, WritableComparable<EdgeKey> {
    String source;
    String target;

    public EdgeKey() {
    }

    public EdgeKey(String source, String target) {
        this.source = source;
        this.target = target;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(source);
        out.writeUTF(target);
    }

    
    public void readFields(DataInput in) throws IOException {
        source = in.readUTF();
        target = in.readUTF();
    }

    @Override
    public EdgeKey clone() {
        try {
            return (EdgeKey) super.clone();
        } catch (CloneNotSupportedException e) {
            System.err.println(e.getStackTrace());
            System.exit(-1);
        }
        return null;
    }

    @Override
    public int compareTo(EdgeKey other) {
        int cmp = this.source.compareTo(other.source);
        if (cmp != 0)
            return cmp;
        return this.target.compareTo(other.target);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || getClass() != obj.getClass())
            return false;
        EdgeKey other = (EdgeKey) obj;
        return this.source.equals(other.source) && this.target.equals(other.target);
    }

    @Override
    public int hashCode() {
        String result = this.source + this.target;
        return result.hashCode();
    }
}