/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.io.pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import edu.umd.cloud9.io.WritableComparatorUtils;

/**
 * WritableComparable representing a pair consisting of an int and a String.
 * The elements in the pair are referred to as the left and right elements. The
 * natural sort order is: first by the left element, and then by the right
 * element.
 * 
 * @author Jimmy Lin
 */
public class PairOfIntString implements WritableComparable<PairOfIntString> {
	private int leftElement;
	private String rightElement;

	/**
	 * Creates a pair.
	 */
	public PairOfIntString() {}

	/**
	 * Creates a pair.
	 *
	 * @param left the left element
	 * @param right the right element
	 */
	public PairOfIntString(int left, String right) {
		set(left, right);
	}

	/**
	 * Deserializes the pair.
	 *
	 * @param in source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		leftElement = in.readInt();
		rightElement = in.readUTF();
	}

	/**
	 * Serializes this pair.
	 *
	 * @param out where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		out.writeInt(leftElement);
		out.writeUTF(rightElement);
	}

	/**
	 * Returns the left element.
	 *
	 * @return the left element
	 */
	public int getLeftElement() {
		return leftElement;
	}

	/**
	 * Returns the right element.
	 *
	 * @return the right element
	 */
	public String getRightElement() {
		return rightElement;
	}

	/**
	 * Returns the key (left element).
	 *
	 * @return the key
	 */
	public int getKey() {
		return leftElement;
	}

	/**
	 * Returns the value (right element).
	 *
	 * @return the value
	 */
	public String getValue() {
		return rightElement;
	}

	/**
	 * Sets the right and left elements of this pair.
	 *
	 * @param left the left element
	 * @param right the right element
	 */
	public void set(int left, String right) {
		leftElement = left;
		rightElement = right;
	}

	/**
	 * Checks two pairs for equality.
	 *
	 * @param obj object for comparison
	 * @return <code>true</code> if <code>obj</code> is equal to this object, <code>false</code> otherwise
	 */
	public boolean equals(Object obj) {
		PairOfIntString pair = (PairOfIntString) obj;
		return leftElement == pair.getLeftElement() && rightElement.equals(pair.getRightElement());
	}

	/**
	 * Defines a natural sort order for pairs. Pairs are sorted first by the
	 * left element, and then by the right element.
	 *
	 * @return a value less than zero, a value greater than zero, or zero if
	 *         this pair should be sorted before, sorted after, or is equal to
	 *         <code>obj</code>.
	 */
	public int compareTo(PairOfIntString pair) {
		int pl = pair.getLeftElement();
		String pr = pair.getRightElement();

		if (leftElement == pl) {
			return rightElement.compareTo(pr);
		}

		return leftElement < pl ? -1 : 1;
	}

	/**
	 * Returns a hash code value for the pair.
	 *
	 * @return hash code for the pair
	 */
	public int hashCode() {
		return leftElement + rightElement.hashCode();
	}

	/**
	 * Generates human-readable String representation of this pair.
	 *
	 * @return human-readable String representation of this pair
	 */
	public String toString() {
		return "(" + leftElement + ", " + rightElement + ")";
	}

	/**
	 * Clones this object.
	 *
	 * @return clone of this object
	 */
	public PairOfIntString clone() {
		return new PairOfIntString(this.leftElement, this.rightElement);
	}

	/** Comparator optimized for <code>PairOfIntString</code>. */
	public static class Comparator extends WritableComparator {

		/**
		 * Creates a new Comparator optimized for <code>PairOfIntString</code>.
		 */
		public Comparator() {
			super(PairOfIntString.class);
		}

		/**
		 * Optimization hook.
		 */
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int thisLeftValue = readInt(b1, s1);
			int thatLeftValue = readInt(b2, s2);

			if (thisLeftValue == thatLeftValue) {
				String thisRightValue = WritableComparatorUtils.readUTF(b1, s1 + 4);
				String thatRightValue = WritableComparatorUtils.readUTF(b2, s2 + 4);

				return thisRightValue.compareTo(thatRightValue);
			}

			return (thisLeftValue < thatLeftValue ? -1 : (thisLeftValue == thatLeftValue ? 0 : 1));
		}
	}

	static { // register this comparator
		WritableComparator.define(PairOfIntString.class, new Comparator());
	}
}
