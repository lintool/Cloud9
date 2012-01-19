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

import edu.umd.cloud9.util.array.ArrayListOfDoubles;

/**
 * Writable extension of the {@code ArrayListOfDoubles} class. This class provides an efficient data
 * structure to store a list of doubles for MapReduce jobs.
 *
 * @author Jimmy Lin
 */
public class ArrayListOfDoublesWritable extends ArrayListOfDoubles
    implements WritableComparable<ArrayListOfDoublesWritable> {

  /**
   * Constructs an {@code ArrayListOfDoublesWritable} object.
   */
  public ArrayListOfDoublesWritable() {
    super();
  }

  /**
   * Constructs an empty list with the specified initial capacity.
   *
   * @param initialCapacity the initial capacity of the list
   */
  public ArrayListOfDoublesWritable(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * Constructs a deep copy of the {@code ArrayListOfDoublesWritable} object given as parameter.
   *
   * @param other object to be copied
   */
  public ArrayListOfDoublesWritable(ArrayListOfDoublesWritable other) {
    super();
    size = other.size();
    array = Arrays.copyOf(other.getArray(), size);
  }

  /**
   * Constructs a list from an array. Defensively makes a copy of the array.
   *
   * @param arr source array
   */
  public ArrayListOfDoublesWritable(double[] arr) {
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
      add(i, in.readDouble());
    }
  }

  /**
   * Serializes this object.
   *
   * @param out where to write the raw byte representation
   */
  public void write(DataOutput out) throws IOException {
    int size = size();
    out.writeInt(size);
    for (int i = 0; i < size; i++) {
      out.writeDouble(get(i));
    }
  }

  @Override
  public String toString(){
    return toString(size());
  }

  /**
   * Creates an instance of this object from {@code ArrayListOfDoubles}. Note that backing array is
   * cloned.
   *
   * @param arr source {@code ArrayListOfDoubles}
   */
  public static ArrayListOfDoublesWritable fromArrayListOfDoubles(ArrayListOfDoubles arr) {
    ArrayListOfDoublesWritable list = new ArrayListOfDoublesWritable();
    list.array = Arrays.copyOf(arr.getArray(), arr.size());
    list.size = arr.size();

    return list;
  }
  
  /**
   * Elementwise comparison. Shorter always comes before if it is a sublist of longer. No preference
   * if both are empty.
   *
   * @param obj other object this is compared against
   */
  @Override
  public int compareTo(ArrayListOfDoublesWritable obj) {
    ArrayListOfDoublesWritable other = (ArrayListOfDoublesWritable) obj;
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
