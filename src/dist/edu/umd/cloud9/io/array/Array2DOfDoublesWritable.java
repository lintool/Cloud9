package edu.umd.cloud9.io.array;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Array2DOfDoublesWritable implements Writable {

	double[][] array;

	/**
	 * Default constructor.
	 */
	public Array2DOfDoublesWritable() {
		super();
	}

	/**
	 * Constructor take in a two-dimensional array. Take note for consistent
	 * purpose, the constructor would re-format the array to a well shaped
	 * 2-dimensional array.
	 * 
	 * @param array
	 *            input double array
	 */
	public Array2DOfDoublesWritable(double[][] array) {
		set(array);
	}

	/**
	 * Constructor that takes the size of the array as an argument.
	 * 
	 */
	public Array2DOfDoublesWritable(int row, int column) {
		super();
		array = new double[row][column];
		for (int i = 0; i < row; i++) {
			array[row] = new double[column];
		}
	}

	public void readFields(DataInput in) throws IOException {
		int row = in.readInt();
		int col = in.readInt();
		array = new double[row][col];
		for (int i = 0; i < row; i++) {
			for (int j = 0; j < col; j++) {
				setValueAt(i, j, in.readDouble());
			}
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(getNumberOfRows());
		out.writeInt(getNumberOfCols());
		for (int i = 0; i < getNumberOfRows(); i++) {
			for (int j = 0; j < getNumberOfCols(); j++) {
				out.writeDouble(getValueAt(i, j));
			}
		}
	}

	public double[][] getClone() {
		return array.clone();
	}

	public double[][] getArray() {
		return array;
	}

	/**
	 * Returns the double value at position <i>i</i>.
	 * 
	 * @param row
	 *            row index of the double to be returned
	 * @param col
	 *            column index of the double to be returned
	 * 
	 * @return double value at position <i>row</i> and <i>col<\i>
	 */
	public double getValueAt(int row, int col) {
		return array[row][col];
	}

	/**
	 * Constructor take in a two-dimensional array. Take note for consistent
	 * purpose, the constructor would re-format the array to a well shaped
	 * 2-dimensional array.
	 * 
	 * @param array
	 *            input double array
	 */
	public void set(double[][] array) {
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
				array[i] = new double[col];
			} else if (array[i].length < col) {
				double[] temp = new double[col];
				for (int j = 0; j < array[i].length; j++) {
					temp[j] = array[i][j];
				}
				array[i] = temp;
			}
		}
	}

	/**
	 * Sets the double at position <i>row</i> and <i>col</i> to <i>f</i>.
	 * 
	 * @param f
	 *            double value to be set
	 */
	public void setValueAt(int row, int col, double f) {
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
