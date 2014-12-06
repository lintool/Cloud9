/*
 * Cloud9: A Hadoop toolkit for working with big data
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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * WritableComparable representing a pair consisting of a String and an int.
 * The elements in the pair are referred to as the left and right elements. The
 * natural sort order is: first by the left element, and then by the right
 * element.
 * 
 * @author Jimmy Lin
 */
public class PairOfStringInt implements WritableComparable<PairOfStringInt> {

	private String leftElement;
	private int rightElement;

	/**
	 * Creates a pair.
	 */
	public PairOfStringInt() {}

	/**
	 * Creates a pair.
	 *
	 * @param left the left element
	 * @param right the right element
	 */
	public PairOfStringInt(String left, int right) {
		set(left, right);
	}

	/**
	 * Deserializes the pair.
	 *
	 * @param in source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		leftElement = Text.readString(in);
		rightElement = in.readInt();
	}

	/**
	 * Serializes this pair.
	 *
	 * @param out where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		Text.writeString(out, leftElement);
		out.writeInt(rightElement);
	}

	/**
	 * Returns the left element.
	 *
	 * @return the left element
	 */
	public String getLeftElement() {
		return leftElement;
	}

	/**
	 * Returns the right element.
	 *
	 * @return the right element
	 */
	public int getRightElement() {
		return rightElement;
	}

	/**
	 * Returns the key (left element).
	 *
	 * @return the key
	 */
	public String getKey() {
		return leftElement;
	}

	/**
	 * Returns the value (right element).
	 *
	 * @return the value
	 */
	public int getValue() {
		return rightElement;
	}

	/**
	 * Sets the right and left elements of this pair.
	 *
	 * @param left the left element
	 * @param right the right element
	 */
	public void set(String left, int right) {
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
		PairOfStringInt pair = (PairOfStringInt) obj;
		return leftElement.equals(pair.getLeftElement()) && rightElement == pair.getRightElement();
	}

	/**
	 * Defines a natural sort order for pairs. Pairs are sorted first by the
	 * left element, and then by the right element.
	 *
	 * @return a value less than zero, a value greater than zero, or zero if
	 *         this pair should be sorted before, sorted after, or is equal to
	 *         <code>obj</code>.
	 */
	public int compareTo(PairOfStringInt pair) {
		String pl = pair.getLeftElement();
		int pr = pair.getRightElement();

		if (leftElement.equals(pl)) {
			if (rightElement == pr)
				return 0;

			return rightElement < pr ? -1 : 1;
		}

		return leftElement.compareTo(pl);
	}

	/**
	 * Returns a hash code value for the pair.
	 *
	 * @return hash code for the pair
	 */
	public int hashCode() {
		return leftElement.hashCode() + rightElement;
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
	public PairOfStringInt clone() {
		return new PairOfStringInt(this.leftElement, this.rightElement);
	}

	/** Comparator optimized for <code>PairOfStringInt</code>. */
	public static class Comparator extends WritableComparator {

		/**
		 * Creates a new Comparator optimized for <code>PairOfStrings</code>.
		 */
		public Comparator() {
			super(PairOfStringInt.class);
		}

    /**
     * Optimization hook.
     */
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try {
        int firstVIntL1 = WritableUtils.decodeVIntSize(b1[s1]);
        int firstVIntL2 = WritableUtils.decodeVIntSize(b2[s2]);
        int firstStrL1 = readVInt(b1, s1);
        int firstStrL2 = readVInt(b2, s2);
        int cmp = compareBytes(b1, s1 + firstVIntL1, firstStrL1, b2, s2 + firstVIntL2, firstStrL2);
        if (cmp != 0) {
          return cmp;
        }

        int thisRightValue = readInt(b1, s1 + firstVIntL1 + firstStrL1);
        int thatRightValue = readInt(b2, s2 + firstVIntL2 + firstStrL2);

        return (thisRightValue < thatRightValue ? -1 : (thisRightValue == thatRightValue ? 0 : 1));
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

	static { // register this comparator
		WritableComparator.define(PairOfStringInt.class, new Comparator());
	}
}
