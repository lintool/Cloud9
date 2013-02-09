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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.io.WritableComparatorUtils;

/**
 * WritableComparable representing a pair consisting of a long and a String.
 * The elements in the pair are referred to as the left and right elements. The
 * natural sort order is: first by the left element, and then by
 * the right element.
 */
public class PairOfLongString implements WritableComparable<PairOfLongString> {
  private long leftElement;
  private String rightElement;

  /**
   * Creates a pair.
   */
  public PairOfLongString() {}

  /**
   * Creates a pair.
   *
   * @param left the left element
   * @param right the right element
   */
  public PairOfLongString(long left, String right) {
    set(left, right);
  }

  /**
   * Deserializes the pair.
   *
   * @param in source for raw byte representation
   */
  public void readFields(DataInput in) throws IOException {
    leftElement = in.readLong();
    rightElement = Text.readString(in);
  }

  /**
   * Serializes this pair.
   *
   * @param out where to write the raw byte representation
   */
  public void write(DataOutput out) throws IOException {
    out.writeLong(leftElement);
    Text.writeString(out, rightElement);
  }

  /**
   * Returns the left element.
   *
   * @return the left element
   */
  public long getLeftElement() {
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
   * Returns the value (right element).
   *
   * @return the value
   */
  public String getValue() {
    return rightElement;
  }

  /**
   * Returns the key (left element).
   *
   * @return the key
   */
  public long getKey() {
    return leftElement;
  }

  /**
   * Sets the right and left elements of this pair.
   *
   * @param left the left element
   * @param right the right element
   */
  public void set(long left, String right) {
    leftElement = left;
    rightElement = right;
  }

  /**
   * Checks two pairs for equality.
   *
   * @param obj object for comparison
   * @return <code>true</code> if <code>obj</code> is equal to this object, <code>false</code> otherwise
   */
  @Override
  public boolean equals(Object obj) {
    PairOfLongString pair = (PairOfLongString) obj;
    return rightElement.equals(pair.getRightElement()) && leftElement == pair.getLeftElement();
  }

  /**
   * Defines a natural sort order for pairs. Pairs are sorted first by the
   * left element, and then by the right element.
   *
   * @return a value less than zero, a value greater than zero, or zero if
   *         this pair should be sorted before, sorted after, or is equal to
   *         <code>obj</code>.
   */
  public int compareTo(PairOfLongString pair) {
    long pl = pair.getLeftElement();
    String pr = pair.getRightElement();

    if (leftElement == pl) {
      if (rightElement.equals(pr))
        return 0;

      return rightElement.compareTo(pr);
    }

    return leftElement < pl ? -1 : 1;
  }

  /**
   * Returns a hash code value for the pair.
   *
   * @return hash code for the pair
   */
  @Override
  public int hashCode() {
    return (int) leftElement + rightElement.hashCode();
  }

  /**
   * Generates human-readable String representation of this pair.
   *
   * @return human-readable String representation of this pair
   */
  @Override
  public String toString() {
    return "(" + leftElement + ", " + rightElement + ")";
  }

  /**
   * Clones this object.
   *
   * @return clone of this object
   */
  @Override
  public PairOfLongString clone() {
    return new PairOfLongString(this.leftElement, this.rightElement);
  }

  /** Comparator optimized for <code>PairOfLongString</code>. */
  public static class Comparator extends WritableComparator {

    /**
     * Creates a new Comparator optimized for <code>PairOfLongString</code>.
     */
    public Comparator() {
      super(PairOfLongString.class);
    }

    /**
     * Optimization hook.
     */
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      long thisLeftValue = readLong(b1, s1);
      long thatLeftValue = readLong(b2, s2);

      if (thisLeftValue == thatLeftValue) {
        int n1 = WritableUtils.decodeVIntSize(b1[s1+8]);
        int n2 = WritableUtils.decodeVIntSize(b2[s2+8]);
        return compareBytes(b1, s1+8+n1, l1-n1-8, b2, s2+n2+8, l2-n2-8);
      }

      return thisLeftValue < thatLeftValue ? -1 : 1;
    }
  }

  static { // register this comparator
    WritableComparator.define(PairOfLongString.class, new Comparator());
  }
}
