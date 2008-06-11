package edu.umd.cloud9.tuple;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PairOfIntsWritableComparable implements WritableComparable {

	private int leftElement, rightElement;

	public PairOfIntsWritableComparable() {
	}

	public PairOfIntsWritableComparable(int left, int right) {
		set(left, right);
	}

	public void readFields(DataInput in) throws IOException {
		leftElement = in.readInt();
		rightElement = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(leftElement);
		out.writeInt(rightElement);
	}

	public int getLeftElement() {
		return leftElement;
	}

	public int getRightElement() {
		return rightElement;
	}

	public void set(int left, int right) {
		leftElement = left;
		rightElement = right;
	}

	public boolean equals(Object obj) {
		PairOfIntsWritableComparable pair = (PairOfIntsWritableComparable) obj;
		return leftElement == pair.getLeftElement()
				&& rightElement == pair.getRightElement();
	}

	public int compareTo(Object obj) {
		PairOfIntsWritableComparable pair = (PairOfIntsWritableComparable) obj;

		int pl = pair.getLeftElement();
		int pr = pair.getRightElement();

		if (leftElement == pl) {
			if (rightElement < pr)
				return -1;
			if (rightElement > pr)
				return 1;
			return 0;
		}

		if (leftElement < pl)
			return -1;

		return 1;
	}

	public int hashCode() {
		return leftElement + rightElement;
	}

	public String toString() {
		return "<" + leftElement + ", " + rightElement + ">";
	}

}
