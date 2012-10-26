package edu.umd.cloud9.io.array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * An array of doubles that implements Writable class.
 * 
 * @author Ke Zhai
 */
public class ArrayOfDoublesWritable implements Writable {
	double[] array;

	/**
	 * Constructor with no arguments.
	 */
	public ArrayOfDoublesWritable() {
		super();
	}

	/**
	 * Constructor take in a one-dimensional array.
	 * 
	 * @param array
	 *            input double array
	 */
	public ArrayOfDoublesWritable(double[] array) {
		this.array = array;
	}

	/**
	 * Constructor that takes the size of the array as an argument.
	 * 
	 * @param size
	 *            number of doubles in array
	 */
	public ArrayOfDoublesWritable(int size) {
		super();
		array = new double[size];
	}

	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		array = new double[size];
		for (int i = 0; i < size; i++) {
			set(i, in.readDouble());
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(size());
		for (int i = 0; i < size(); i++) {
			out.writeDouble(get(i));
		}
	}

	/**
	 * Get a deep copy of the array.
	 * 
	 * @return a clone of the array
	 */
	public double[] getClone() {
		return array.clone();
	}

	/**
	 * Get a shallow copy of the array.
	 * 
	 * @return a pointer to the array
	 */
	public double[] getArray() {
		return array;
	}

	/**
	 * Set the array.
	 * 
	 * @param array
	 */
	public void setArray(double[] array) {
		this.array = array;
	}

	/**
	 * Returns the double value at position <i>i</i>.
	 * 
	 * @param i
	 *            index of double to be returned
	 * @return double value at position <i>i</i>
	 */
	public double get(int i) {
		return array[i];
	}

	/**
	 * Sets the double at position <i>i</i> to <i>f</i>.
	 * 
	 * @param i
	 *            position in array
	 * @param f
	 *            double value to be set
	 */
	public void set(int i, double f) {
		array[i] = f;
	}

	/**
	 * Returns the size of the double array.
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
