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
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;

/**
 * <p>
 * Class that represents a list in Hadoop's data type system. Elements in the
 * list must be homogeneous and must implement Hadoop's WritableComparable
 * interface. This class, combined with {@link Tuple}, allows the user to
 * define arbitrarily complex data structures.
 * </p>
 * 
 * @see Tuple
 * @param <E>
 *            type of list element
 */
public class ListWritable<E extends WritableComparable> implements
		WritableComparable,
		Iterable<E> {

	private List<E> mList;

	/**
	 * Creates a ListWritable object.
	 */
	public ListWritable() {
		mList = new ArrayList<E>();
	}

	/**
	 * Appends the specified element to the end of this list.
	 * 
	 * @param e
	 *            element to be appended to this list
	 */
	public void add(E e) {
		mList.add(e);
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
	 * Removes all elements from this list.
	 */
	public void clear() {
		mList.clear();
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
		
		mList.clear();
		
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

	/**
	 * <p>
	 * Defines a natural sort order for the ListWritable class. Following
	 * standard convention, this method returns a value less than zero, a value
	 * greater than zero, or zero if this ListWritable should be sorted before,
	 * sorted after, or is equal to <code>obj</code>. The sort order is
	 * defined as follows:
	 * </p>
	 * 
	 * <ul>
	 * <li>Each element in the list is compared sequentially from first to
	 * last.</li>
	 * <li>Lists are sorted with respect to the natural order of the current
	 * list element under consideration, by calling its <code>compareTo</code>
	 * method.</li>
	 * <li>If the current list elements are equal, the next set of elements are
	 * considered.</li>
	 * <li>If all compared elements are equal, but lists are different lengths,
	 * the shorter list is sorted first.</li>
	 * <li>If all list elements are equal and the lists are equal in length,
	 * then the lists are considered equal</li>
	 * </ul>
	 * 
	 * @return a value less than zero, a value greater than zero, or zero if
	 *         this Tuple should be sorted before, sorted after, or is equal to
	 *         <code>obj</code>.
	 */
	public int compareTo(Object obj) {
		ListWritable<?> that = (ListWritable<?>) obj;

		// iterate through the fields
		for (int i = 0; i < this.size(); i++) {
			// sort shorter list first
			if (i >= that.size())
				return 1;

			@SuppressWarnings("unchecked")
			Comparable<Object> thisField = this.get(i);
			@SuppressWarnings("unchecked")
			Comparable<Object> thatField = that.get(i);

			if (thisField.equals(thatField)) {
				// if we're down to the last field, sort shorter list first
				if (i == this.size() - 1) {
					if (this.size() > that.size())
						return 1;

					if (this.size() < that.size())
						return -1;
				}
				// otherwise, move to next field
			} else {
				return thisField.compareTo(thatField);
			}
		}

		return 0;
	}

	/**
	 * @return an iterator over the elements in this list in proper sequence.
	 */
	public Iterator<E> iterator() {
		return this.mList.iterator();
	}
}
