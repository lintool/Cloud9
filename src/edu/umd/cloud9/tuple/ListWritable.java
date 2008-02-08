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

package edu.umd.cloud9.tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * <p>
 * Class that represents a list in Hadoop's data type system. Elements in the
 * list must implement Hadoop's WritableComparable interface. This class,
 * combined with {@link Tuple}, allows the user to define arbitrarily complex
 * data structures.
 * </p>
 * 
 * @see Tuple
 * @param <E>
 *            type of list element
 */
public class ListWritable<E extends WritableComparable> implements
		WritableComparable {

	private List<E> mList;

	/**
	 * Creates a ListWritable object.
	 */
	public ListWritable() {
		mList = new ArrayList<E>();
	}

	/**
	 * Returns the element at the specified position in this list
	 * 
	 * @param index
	 *            index of the element to return
	 * @return the element at the specified position in this list
	 */
	public E get(int index) {
		if (index < 0 || index >= mList.size()) {
			throw new IndexOutOfBoundsException();
		}

		return mList.get(index);
	}

	/**
	 * Appends the specified element to the end of this list.
	 * 
	 * @param e
	 *            element to be appended to this list
	 */
	public void add(E e) {
		mBytes = null;
		mList.add(e);
	}

	/**
	 * Replaces the element at the specified position in this list with the
	 * specified element.
	 * 
	 * @param index
	 *            index of the element to replace
	 * @param element
	 *            element to be stored at the specified position
	 */
	public E set(int index, E element) {
		mBytes = null;
		return mList.set(index, element);
	}

	/**
	 * Returns the number of elements in this list.
	 * 
	 * @return the number of elements in this list
	 */
	public int size() {
		return mList.size();
	}

	/**
	 * Deserializes the Tuple.
	 * 
	 * @param in
	 *            source for raw byte representation
	 */
	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		int numFields = in.readInt();

		for (int i = 0; i < numFields; i++) {
			try {
				String className = in.readUTF();

				int sz = in.readInt();
				byte[] bytes = new byte[sz];
				in.readFully(bytes);

				E obj = (E) Class.forName(className).newInstance();
				obj.readFields(new DataInputStream(new ByteArrayInputStream(
						bytes)));
				this.add(obj);
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
	}

	/**
	 * Serializes this Tuple.
	 * 
	 * @param out
	 *            where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		out.writeInt(mList.size());

		for (int i = 0; i < mList.size(); i++) {
			if (mList.get(i) == null) {
				throw new TupleException("Cannot serialize null fields!");
			}

			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(bytesOut);

			mList.get(i).write(dataOut);

			out.writeUTF(mList.get(i).getClass().getCanonicalName());
			out.writeInt(bytesOut.size());
			out.write(bytesOut.toByteArray());
		}
	}

	/**
	 * Generates human-readable String representation of this Tuple.
	 * 
	 * @return human-readable String representation of this Tuple
	 */
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (int i = 0; i < this.size(); i++) {
			if (i != 0)
				sb.append(", ");
			sb.append(this.get(i));
		}
		sb.append("]");

		return sb.toString();
	}

	private byte[] mBytes = null;

	private byte[] getBytes() {
		if (mBytes == null)
			generateByteRepresentation();

		return mBytes;
	}

	private void generateByteRepresentation() {
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(byteStream);
		try {
			this.write(out);
		} catch (IOException e) {
			e.printStackTrace();
		}
		mBytes = byteStream.toByteArray();
	}

	public int compareTo(Object obj) {
		byte[] thoseBytes = ((ListWritable<?>) obj).getBytes();
		byte[] theseBytes = this.getBytes();

		return WritableComparator.compareBytes(theseBytes, 0,
				theseBytes.length, thoseBytes, 0, thoseBytes.length);
	}
}
