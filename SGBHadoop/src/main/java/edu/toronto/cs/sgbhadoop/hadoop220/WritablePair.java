package edu.toronto.cs.sgbhadoop.hadoop220;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class WritablePair implements WritableComparable<WritablePair> {
	public String a;
	public String b;

	public String getA() {
		return a;
	}

	public void setA(String a) {
		this.a = a;
	}

	public String getB() {
		return b;
	}

	public void setB(String b) {
		this.b = b;
	}

	public WritablePair() {

	}

	public WritablePair(String a, String b) {
		this.a = a;
		this.b = b;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, a);
		Text.writeString(out, b);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		a = Text.readString(in);
		b = Text.readString(in);
	}

	@Override
	public int compareTo(WritablePair o) {
		int result = a.compareTo(o.a);
		if (result == 0) {
			result = b.compareTo(o.b);
		}
		return result;
	}

	@Override
	public String toString() {
		return "writablepair:" + a + ", " + b;
	}
}