package edu.umd.cloud9.tuple;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PairOfStringsWritableComparable implements WritableComparable {

	private String leftElement, rightElement;

	public PairOfStringsWritableComparable() {
	}

	public PairOfStringsWritableComparable(String left, String right) {
		set(left, right);
	}

	public boolean equals(Object obj) {
		PairOfStringsWritableComparable pair = (PairOfStringsWritableComparable) obj;
		return leftElement.equals(pair.getLeftElement())
				&& rightElement.equals(pair.getRightElement());
	}

	public void readFields(DataInput in) throws IOException {
		leftElement = in.readUTF();
		rightElement = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(leftElement);
		out.writeUTF(rightElement);
	}

	public String getLeftElement() {
		return leftElement;
	}

	public String getRightElement() {
		return rightElement;
	}

	public int compareTo(Object obj) {
		PairOfStringsWritableComparable pair = (PairOfStringsWritableComparable) obj;

		String pl = pair.getLeftElement();
		String pr = pair.getRightElement();

		if (leftElement.equals(pl)) {
			return rightElement.compareTo(pr);
		}

		return leftElement.compareTo(pl);
	}

	public void set(String left, String right) {
		leftElement = left;
		rightElement = right;
	}

	public int hashCode() {
		return leftElement.hashCode() + rightElement.hashCode();
	}

	public String toString() {
		return "<" + leftElement + ", " + rightElement + ">";
	}

}
