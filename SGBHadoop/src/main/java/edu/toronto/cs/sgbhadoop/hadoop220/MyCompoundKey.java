package edu.toronto.cs.sgbhadoop.hadoop220;

import static org.junit.Assert.assertTrue;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

public class MyCompoundKey implements WritableComparable<MyCompoundKey> {
	public String s;
	public int order;

	public String getS() {
		return s;
	}

	public void setS(String s) {
		this.s = s;
	}

	public int getOrder() {
		return order;
	}

	public void setOrder(int order) {
		this.order = order;
	}

	@Override
	public String toString() {
		return "mycompoundkey: " + s + ", " + order;
	}

	public MyCompoundKey() {

	}

	public MyCompoundKey(String s, int order) {
		this.s = s;
		this.order = order;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, s);
		out.writeInt(order);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		s = Text.readString(in);
		order = in.readInt();
	}

	@Override
	public int compareTo(MyCompoundKey o) {
		int result = this.s.compareTo(o.s);
		if (result == 0) {
			return order - o.order;
		}
		return result;
	}

	@Test
	public void testMyCompoundKey() {
		MyCompoundKey k1 = new MyCompoundKey("s1", 0);
		MyCompoundKey k4 = new MyCompoundKey("s1", 0);
		MyCompoundKey k2 = new MyCompoundKey("s2", 0);
		MyCompoundKey k3 = new MyCompoundKey("s1", 1);

		// result: ( k1 == k1 ) && (k1 == k4 ) && (k1 || k4 ) < k2 < k3
		assertTrue(k1.compareTo(k1) == 0);
		assertTrue(k1.compareTo(k4) == 0);
		assertTrue(k1.compareTo(k2) == -1);
		assertTrue(k1.compareTo(k3) == -1);
		assertTrue(k2.compareTo(k3) == 1);

		// inverse
		assertTrue(k4.compareTo(k1) == 0);
		assertTrue(k2.compareTo(k1) == 1);
		assertTrue(k3.compareTo(k1) == 1);
		assertTrue(k3.compareTo(k2) == -1);

	}
}