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

package edu.umd.cloud9.io.array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;

import edu.umd.cloud9.util.array.ArrayListOfInts;

/**
 * Writable extension of the {@code ArrayListOfInts} class. This class provides an efficient data
 * structure to store a list of ints for MapReduce jobs.
 *
 * @author Jimmy Lin
 */
public class ArrayListOfIntsWritable extends ArrayListOfInts
    implements WritableComparable<ArrayListOfIntsWritable> {

  /**
   * Constructs an ArrayListOfIntsWritable object.
   */
  public ArrayListOfIntsWritable() {
    super();
  }

  /**
   * Constructs an empty list with the specified initial capacity.
   *
   * @param initialCapacity	the initial capacity of the list
   */
  public ArrayListOfIntsWritable(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * Constructs a list populated with shorts in range [first, last).
   *
   * @param first the smallest short in the range (inclusive)
   * @param last  the largest short in the range (exclusive)
   */
  public ArrayListOfIntsWritable(int first, int last) {
    super(first, last);
  }

  /**
   * Constructs a deep copy of the ArrayListOfIntsWritable object 
   * given as parameter.
   *
   * @param other object to be copied
   */
  public ArrayListOfIntsWritable(ArrayListOfIntsWritable other) {
    super();
    size = other.size();
    array = Arrays.copyOf(other.getArray(), size);
  }

  /**
   * Constructs a list from an array. Defensively makes a copy of the array.
   *
   * @param arr source array
   */
  public ArrayListOfIntsWritable(int[] arr) {
    super(arr);
  }

  /**
   * Deserializes this object.
   *
   * @param in source for raw byte representation
   */
  public void readFields(DataInput in) throws IOException {
    this.clear();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      add(i, in.readInt());
    }
  }

  /**
   * Serializes this object.
   *
   * @param out	where to write the raw byte representation
   */
  public void write(DataOutput out) throws IOException {
    int size = size();
    out.writeInt(size);
    for (int i = 0; i < size; i++) {
      out.writeInt(get(i));
    }
  }

  @Override
  public String toString(){
    return toString(size());
  }

  /**
   * Creates a Writable version of this list.
   */
  public static ArrayListOfIntsWritable fromArrayListOfInts(ArrayListOfInts a) {
    ArrayListOfIntsWritable list = new ArrayListOfIntsWritable();
    list.array = Arrays.copyOf(a.getArray(), a.size());
    list.size = a.size();

    return list;
  }

  /**
   * Elementwise comparison. Shorter always comes before if it is a sublist of longer. No preference
   * if both are empty.
   * 
   * @param obj other object this is compared against
   */
  @Override
  public int compareTo(ArrayListOfIntsWritable obj) {
    ArrayListOfIntsWritable other = (ArrayListOfIntsWritable) obj;
    if (isEmpty()) {
      if (other.isEmpty()) {
        return 0;
      } else {
        return -1;
      }
    }

    for (int i = 0; i < size(); i++) {
      if (other.size() <= i) {
        return 1;
      }
      if (get(i) < other.get(i)) {
        return -1;
      } else if (get(i) > other.get(i)) {
        return 1;
      }
    }

    if (other.size() > size()) {
      return -1;
    } else {
      return 0;
    }
  }
}
