import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class CalculateMedNSD implements Writable {

	private long stdDev = 0;
	private long median = 0;
	private long variance = 0;
	
	
	public void readFields(DataInput in) throws IOException {
		stdDev = in.readLong();
		median = in.readLong();
		variance = in.readLong();
	}

	public void write(DataOutput out) throws IOException {
		out.writeLong(stdDev);
		out.writeLong(median);
		out.writeLong(variance);
	}

	public void setStdDev(long stdDev) {
		this.stdDev = stdDev;
	}
	
	public void setMedian(long median) {
		this.median = median;
	}
	
	public void setVariance(long variance) {
		this.variance = variance;
	}
	
	public long getStdDev() {
		return stdDev;
	}
	
	public long getMedian() {
		return median;
	}
	
	public long getVariance() {
		return variance;
	}
}
