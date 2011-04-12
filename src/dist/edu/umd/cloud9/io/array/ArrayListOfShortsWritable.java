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

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.util.array.ArrayListOfShorts;

/**
 * Writable extension of the ArrayListOfShorts class. This class provides an
 * efficient data structure to store a list of ints for MapReduce jobs.
 * 
 * @author Ferhan Ture
 */
public class ArrayListOfShortsWritable extends ArrayListOfShorts implements Writable {

  /**
   * Constructs an ArrayListOfIntsWritable object.
   */
  public ArrayListOfShortsWritable() {
    super();
  }

  /**
   * Constructs an ArrayListOfIntsWritable object from a given integer range [
   * first , last ). The created list includes the first parameter but excludes
   * the second.
   * 
   * @param firstNumber
   *          the smallest integer in the range
   * @param lastNumber
   *          the largest integer in the range
   */
  public ArrayListOfShortsWritable(short firstNumber, short lastNumber) {
    super();
    int j = 0;
    for (short i = firstNumber; i < lastNumber; i++) {
      this.add(j++, i);
    }
  }

  /**
   * Constructs an empty list with the specified initial capacity.
   * 
   * @param initialCapacity
   *          the initial capacity of the list
   */
  public ArrayListOfShortsWritable(int initialCapacity) {
    super(initialCapacity);
  }

  /**
   * Constructs a deep copy of the ArrayListOfIntsWritable object given as
   * parameter.
   * 
   * @param other
   *          object to be copied
   */
  public ArrayListOfShortsWritable(ArrayListOfShortsWritable other) {
    super();
    for (int i = 0; i < other.size(); i++) {
      add(i, other.get(i));
    }
  }

  public ArrayListOfShortsWritable(short[] perm) {
    super();
    for (int i = 0; i < perm.length; i++) {
      add(i, perm[i]);
    }
  }

  /**
   * Deserializes this object.
   * 
   * @param in
   *          source for raw byte representation
   */
  public void readFields(DataInput in) throws IOException {
    this.clear();
    int size = in.readInt();
    for (int i = 0; i < size; i++) {
      add(i, in.readShort());
    }
  }

  /**
   * Serializes this object.
   * 
   * @param out
   *          where to write the raw byte representation
   */
  public void write(DataOutput out) throws IOException {
    int size = size();
    out.writeInt(size);
    for (int i = 0; i < size; i++) {
      out.writeShort(get(i));
    }
  }

  @Override
  public String toString(){
    return toString(size());
  }
}
