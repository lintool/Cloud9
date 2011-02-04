package edu.umd.cloud9.io.pair;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.util.map.HMapII;
import edu.umd.cloud9.util.map.MapII;

/**
 * 
 * @author kzhai
 */
public class PairOfHMapIIs implements Writable {

	private static boolean sLazyDecode = false;

	private HMapII leftElement = new HMapII();
	private HMapII rightElement = new HMapII();

	private int leftNumEntries = 0;
	private int rightNumEntries = 0;

	private int[] leftKeys = null;
	private int[] leftValues = null;

	private int[] rightKeys = null;
	private int[] rightValues = null;

	/**
	 * Creates a pair.
	 */
	public PairOfHMapIIs() {
	}

	/**
	 * Creates a pair.
	 * 
	 * @param left
	 *            the left element
	 * @param right
	 *            the right element
	 */
	public PairOfHMapIIs(HMapII left, HMapII right) {
		set(left, right);
	}

	/**
	 * Deserializes the pair.
	 * 
	 * @param in
	 *            source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		leftElement.clear();

		leftNumEntries = in.readInt();
		if (leftNumEntries != 0) {
			if (sLazyDecode) {
				// lazy initialization; read into arrays
				leftKeys = new int[leftNumEntries];
				leftValues = new int[leftNumEntries];

				for (int i = 0; i < leftNumEntries; i++) {
					leftKeys[i] = in.readInt();
					leftValues[i] = in.readInt();
				}
			} else {
				// normal initialization; populate the map
				for (int i = 0; i < leftNumEntries; i++) {
					leftElement.put(in.readInt(), in.readInt());
				}
			}
		}

		rightElement.clear();

		rightNumEntries = in.readInt();
		if (rightNumEntries != 0) {
			if (sLazyDecode) {
				// lazy initialization; read into arrays
				rightKeys = new int[rightNumEntries];
				rightValues = new int[rightNumEntries];

				for (int i = 0; i < rightNumEntries; i++) {
					rightKeys[i] = in.readInt();
					rightValues[i] = in.readInt();
				}
			} else {
				// normal initialization; populate the map
				for (int i = 0; i < rightNumEntries; i++) {
					rightElement.put(in.readInt(), in.readInt());
				}
			}
		}
	}

	/**
	 * Serializes this pair.
	 * 
	 * @param out
	 *            where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		// Write out the number of entries in the left map element
		out.writeInt(leftElement.size());
		if (leftElement.size() != 0) {
			for (MapII.Entry e : leftElement.entrySet()) {
				out.writeInt(e.getKey());
				out.writeInt(e.getValue());
			}
		}

		// Write out the number of entries in the left map element
		out.writeInt(rightElement.size());
		if (rightElement.size() != 0) {
			for (MapII.Entry e : rightElement.entrySet()) {
				out.writeInt(e.getKey());
				out.writeInt(e.getValue());
			}
		}
	}

	/**
	 * In lazy decoding mode, populates the map with deserialized data.
	 * Otherwise, does nothing.
	 * 
	 * @throws IOException
	 */
	public void decode() throws IOException {
		if (leftKeys != null && leftNumEntries != 0) {
			for (int i = 0; i < leftKeys.length; i++) {
				leftElement.put(leftKeys[i], leftValues[i]);
			}
		}

		if (rightKeys != null && rightNumEntries != 0) {
			for (int i = 0; i < rightKeys.length; i++) {
				rightElement.put(rightKeys[i], rightValues[i]);
			}
		}
	}

	/**
	 * Returns the serialized representation of this object as a byte array.
	 * 
	 * @return byte array representing the serialized representation of this
	 *         object
	 * @throws IOException
	 */
	public byte[] serialize() throws IOException {
		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);
		write(dataOut);

		return bytesOut.toByteArray();
	}

	/**
	 * Creates a <code>PairOfOHMapIIs</code> object from a
	 * <code>DataInput</code>.
	 * 
	 * @param in
	 *            <code>DataInput</code> for reading the serialized
	 *            representation
	 * @return a newly-created <code>PairOfOHMapIIs</code> object
	 * @throws IOException
	 */
	public static PairOfHMapIIs create(DataInput in) throws IOException {
		PairOfHMapIIs m = new PairOfHMapIIs();
		m.readFields(in);

		return m;
	}

	/**
	 * Creates a <code>PairOfOHMapIIs</code> object from a byte array.
	 * 
	 * @param bytes
	 *            raw serialized representation
	 * @return a newly-created <code>PairOfOHMapIIs</code> object
	 * @throws IOException
	 */
	public static PairOfHMapIIs create(byte[] bytes) throws IOException {
		return create(new DataInputStream(new ByteArrayInputStream(bytes)));
	}

	/**
	 * Sets the lazy decoding flag.
	 * 
	 * @param b
	 *            the value of the lazy decoding flag
	 */
	public static void setLazyDecodeFlag(boolean b) {
		sLazyDecode = b;
	}

	/**
	 * Returns the value of the lazy decoding flag
	 * 
	 * @return the value of the lazy decoding flag
	 */
	public static boolean getLazyDecodeFlag() {
		return sLazyDecode;
	}

	/**
	 * Returns the left element.
	 * 
	 * @return the left element
	 */
	public HMapII getLeftElement() {
		return leftElement;
	}

	/**
	 * Returns the right element.
	 * 
	 * @return the right element
	 */
	public HMapII getRightElement() {
		return rightElement;
	}

	/**
	 * Sets the right and left elements of this pair.
	 * 
	 * @param left
	 *            the left element
	 * @param right
	 *            the right element
	 */
	public void set(HMapII left, HMapII right) {
		leftElement = left;
		rightElement = right;
	}

	/**
	 * Generates human-readable String representation of this pair.
	 * 
	 * @return human-readable String representation of this pair
	 */
	public String toString() {
		String leftString = "[\t";
		String rightString = "[\t";

		if (sLazyDecode) {
			for (int i = 0; i < leftNumEntries; i++) {
				leftString += leftKeys[i] + ":" + leftValues[i] + "\t";
			}
			leftString += "]";

			for (int i = 0; i < rightNumEntries; i++) {
				rightString += rightKeys[i] + ":" + rightValues[i] + "\t";
			}
			rightString += "]";
		} else {
			for (MapII.Entry e : leftElement.entrySet()) {
				leftString += e.getKey() + ":" + e.getValue() + "\t";
			}
			leftString += "]";

			for (MapII.Entry e : rightElement.entrySet()) {
				rightString += e.getKey() + ":" + e.getValue() + "\t";
			}
			rightString += "]";
		}

		return leftString + "\n" + rightString;
	}

	/**
	 * Clones this object.
	 * 
	 * @return clone of this object
	 */
	public PairOfHMapIIs clone() {
		return new PairOfHMapIIs((HMapII) this.leftElement.clone(),
				(HMapII) this.rightElement.clone());
	}
}
