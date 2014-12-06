package edu.umd.cloud9.io.array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Array2DOfFloatsWritable implements Writable {

	float[][] array;

	/**
	 * Default constructor.
	 */
	public Array2DOfFloatsWritable() {
		super();
	}

	/**
	 * Constructor take in a two-dimensional array. Take note for consistent
	 * purpose, the constructor would re-format the array to a well shaped
	 * 2-dimensional array.
	 * 
	 * @param array
	 *            input float array
	 */
	public Array2DOfFloatsWritable(float[][] array) {
		set(array);
	}

	/**
	 * Constructor that takes the size of the array as an argument.
	 * 
	 */
	public Array2DOfFloatsWritable(int row, int column) {
		super();
		array = new float[row][column];
		for (int i = 0; i < row; i++) {
			array[row] = new float[column];
		}
	}

	public void readFields(DataInput in) throws IOException {
		int row = in.readInt();
		int col = in.readInt();
		array = new float[row][col];
		for (int i = 0; i < row; i++) {
			for (int j = 0; j < col; j++) {
				setValueAt(i, j, in.readFloat());
			}
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(getNumberOfRows());
		out.writeInt(getNumberOfCols());
		for (int i = 0; i < getNumberOfRows(); i++) {
			for (int j = 0; j < getNumberOfCols(); j++) {
				out.writeFloat(getValueAt(i, j));
			}
		}
	}

	public float[][] getClone() {
		return array.clone();
	}

	public float[][] getArray() {
		return array;
	}

	/**
	 * Returns the float value at position <i>i</i>.
	 * 
	 * @param row
	 *            row index of the float to be returned
	 * @param col
	 *            column index of the float to be returned
	 * 
	 * @return float value at position <i>row</i> and <i>col<\i>
	 */
	public float getValueAt(int row, int col) {
		return array[row][col];
	}

	/**
	 * Constructor take in a two-dimensional array. Take note for consistent
	 * purpose, the constructor would re-format the array to a well shaped
	 * 2-dimensional array.
	 * 
	 * @param array
	 *            input float array
	 */
	public void set(float[][] array) {
		int row = array.length;
		int col = 0;
		for (int i = 0; i < row; i++) {
			if (array[i] != null) {
				col = Math.max(col, array[i].length);
			}
		}
		this.array = array;

		for (int i = 0; i < row; i++) {
			if (array[i] == null) {
				array[i] = new float[col];
			} else if (array[i].length < col) {
				float[] temp = new float[col];
				for (int j = 0; j < array[i].length; j++) {
					temp[j] = array[i][j];
				}
				array[i] = temp;
			}
		}
	}

	/**
	 * Sets the float at position <i>row</i> and <i>col</i> to <i>f</i>.
	 * 
	 * @param f
	 *            float value to be set
	 */
	public void setValueAt(int row, int col, float f) {
		array[row][col] = f;
	}

	/**
	 * Returns the number of rows in the float array.
	 * 
	 * @return number of rows in array
	 */
	public int getNumberOfRows() {
		return array.length;
	}

	/**
	 * Returns the number of columns in the array.
	 * 
	 * @return number of columns in the array
	 */
	public int getNumberOfCols() {
		return array[0].length;
	}

	public String toString() {
		String s = "[";
		for (int i = 0; i < getNumberOfRows(); i++) {
			for (int j = 0; j < getNumberOfCols() - 1; j++) {
				s += getValueAt(i, j) + ", ";
			}
			s += getValueAt(i, getNumberOfCols() - 1) + "; ";
		}
		s += "]";
		return s;
	}
}
