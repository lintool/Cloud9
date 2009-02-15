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

package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * A three-byte version of IntWritable.
 */
public class Int3Writable implements WritableComparable {

	private int value;

	/**
	 * Creates an Int3Writable.
	 */
	public Int3Writable() {
	}

	/**
	 * Creates an Int3Writable with an initial value.
	 */
	public Int3Writable(int value) {
		this.value = value;
	}

	/**
	 * Deserializes this pair.
	 * 
	 * @param in
	 *            source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		value = (in.readByte() & 0xFF) << 16 | (in.readByte() & 0xFF) << 8 | in.readByte() & 0xFF;
	}

	/**
	 * Serializes this pair.
	 * 
	 * @param out
	 *            where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		out.writeByte((byte) (0xff & (value >> 16)));
		out.writeByte((byte) (0xff & (value >> 8)));
		out.writeByte((byte) (0xff & value));
	}

	/**
	 * Sets the int value of this Int3Writable.
	 * 
	 * @param value
	 *            int value
	 */
	public void set(int value) {
		this.value = value;
	}

	/**
	 * Returns the int value of this Int3Writable.
	 * 
	 * @return int value of this Int3Writable
	 */
	public int get() {
		return value;
	}

	/**
	 * Compares two Int3Writable.
	 */
	public int compareTo(Object o) {
		int thisValue = this.value;
		int thatValue = ((Int3Writable) o).value;

		return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
	}

	/**
	 * Clones this object.
	 * 
	 * @return clone of this object
	 */
	public Int3Writable clone() {
		return new Int3Writable(this.value);
	}

}
