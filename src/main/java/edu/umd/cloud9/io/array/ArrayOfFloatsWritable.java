package edu.umd.cloud9.io.array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * An array of floats that implements Writable class.
 * 
 * @author Ferhan Ture
 */
public class ArrayOfFloatsWritable implements Writable {
	float[] array;

	/**
	 * Constructor with no arguments.
	 */
	public ArrayOfFloatsWritable() {
		super();
	}

	/**
	 * Constructor take in a one-dimensional array.
	 * 
	 * @param array
	 *            input float array
	 */
	public ArrayOfFloatsWritable(float[] array) {
		this.array = array;
	}

	/**
	 * Constructor that takes the size of the array as an argument.
	 * 
	 * @param size
	 *            number of floats in array
	 */
	public ArrayOfFloatsWritable(int size) {
		super();
		array = new float[size];
	}

	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		array = new float[size];
		for (int i = 0; i < size; i++) {
			set(i, in.readFloat());
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(size());
		for (int i = 0; i < size(); i++) {
			out.writeFloat(get(i));
		}
	}

	/**
	 * Get a deep copy of the array.
	 * 
	 * @return a clone of the array
	 */
	public float[] getClone() {
		return array.clone();
	}

	/**
	 * Get a shallow copy of the array.
	 * 
	 * @return a pointer to the array
	 */
	public float[] getArray() {
		return array;
	}

	/**
	 * Set the array.
	 * 
	 * @param array
	 */
	public void setArray(float[] array) {
		this.array = array;
	}

	/**
	 * Returns the float value at position <i>i</i>.
	 * 
	 * @param i
	 *            index of float to be returned
	 * @return float value at position <i>i</i>
	 */
	public float get(int i) {
		return array[i];
	}

	/**
	 * Sets the float at position <i>i</i> to <i>f</i>.
	 * 
	 * @param i
	 *            position in array
	 * @param f
	 *            float value to be set
	 */
	public void set(int i, float f) {
		array[i] = f;
	}

	/**
	 * Returns the size of the float array.
	 * 
	 * @return size of array
	 */
	public int size() {
		return array.length;
	}

	public String toString() {
		String s = "[";
		for (int i = 0; i < size(); i++) {
			s += get(i) + ",";
		}
		s += "]";
		return s;
	}
}
