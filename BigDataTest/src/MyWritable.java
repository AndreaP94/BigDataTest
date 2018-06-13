import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class MyWritable implements WritableComparable<MyWritable> {

	private Text solver;
	private Text time;

	protected MyWritable() {
		solver = new Text("");
		time = new Text("");
	}

	protected MyWritable(Text solver, Text time) {
		this.solver = solver;
		this.time = time;
	}

	public Text getSolver() {
		return solver;
	}

	public Text getTime() {
		return time;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		solver.readFields(arg0);
		time.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		solver.write(arg0);
		time.write(arg0);
	}

	public static MyWritable read(Text solver, Text time) throws IOException {
		MyWritable w = new MyWritable(solver, time);
		return w;

	}

	@Override
	public int compareTo(MyWritable w) {
		/*
		 * int compareTag = tag.compareTo(o.tag); int compareValues =
		 * value.compareTo(o.value); return compareTag != 0 ? compareTag :
		 * compareValues;
		 * 
		 * 
		 * if(this.solver.equals(w.getSolver())) { if(this.time < w.getTime()) return 1
		 * }
		 */

		int compareTag = solver.compareTo(w.getSolver());

		double time_one = Double.parseDouble(time.toString());
		double time_two = Double.parseDouble(w.getTime().toString());

		if (compareTag == 0) {
			if (time_one > time_two) {
				return 1;
			} else if (time_one == time_two) {
				return 0;
			} else
				return -1;
		} else
			return compareTag;
	}

}
