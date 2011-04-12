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

import edu.umd.cloud9.util.array.ArrayListOfInts;

/**
 * Writable extension of the ArrayListOfInts class. This class provides an
 * efficient data structure to store a list of ints for MapReduce jobs.
 *
 * @author Ferhan Ture
 */
public class ArrayListOfIntsWritable extends ArrayListOfInts implements Writable {

	/**
	 * Constructs an ArrayListOfIntsWritable object.
	 */
	public ArrayListOfIntsWritable() {
		super();
	}

	public ArrayListOfIntsWritable(int firstNumber, int lastNumber) {
		super(firstNumber, lastNumber);
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
	 * Constructs a deep copy of the ArrayListOfIntsWritable object 
	 * given as parameter.
	 *
	 * @param other object to be copied
	 */
	public ArrayListOfIntsWritable(ArrayListOfIntsWritable other) {
    super();
    for (int i = 0; i < other.size(); i++) {
      add(i, other.get(i));
    }
  }

  /**
   * Constructs a list with the array backing the object. Beware when
   * subsequently manipulating the array.
   *
   * @param arr backing array
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
		for(int i=0;i<size;i++){
			add(i,in.readInt());
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
		for(int i=0;i<size;i++){
			out.writeInt(get(i));
		}
	}

	@Override
	public String toString(){
	  return toString(size());
	}

	public static ArrayListOfIntsWritable fromArrayListOfInts(ArrayListOfInts a) {
	  ArrayListOfIntsWritable list = new ArrayListOfIntsWritable();
	  list.array = a.getArray();
	  list.size = a.size();

	  return list;
	}
}
