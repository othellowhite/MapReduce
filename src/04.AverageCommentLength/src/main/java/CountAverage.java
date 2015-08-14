import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class CountAverage implements Writable {
	private int count = 0;
	private int length = 0;
	
	public void setCount(int count) {
		this.count = count;
	}
	
	public void setLength(int length) {
		this.length = length;
	}
	
	public float getCount() {
		return count;
	}
	
	public float getLength() {
		return length;
	}

	public void readFields(DataInput in) throws IOException {
		count = in.readInt();
		length = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(count);
		out.writeInt(length);
	}
	
	public String toString() {
		return count + "\t" + length;
	}
}