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

package src.edu.umd.cloud9.tuple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

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
public class ListWritable<E extends WritableComparable> implements WritableComparable, Iterable<E>, List<E> {

	private List<E> mList;

	private Class<?> listElementClass;

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
	public boolean add(E e) {
		if (mList.size() == 0) 
			listElementClass = e.getClass();
		else if (!e.getClass().equals(listElementClass))
			throw new IllegalArgumentException("Cannot add element of type " + e.getClass().getCanonicalName() + " to list of type " + listElementClass.getCanonicalName());
		return mList.add(e);
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
		String className = in.readUTF();
		E obj;
		try {
			Class c = Class.forName(className);
			listElementClass = c;

			for (int i = 0; i < numFields; i++) {
				obj = (E) c.newInstance();
				int sz = in.readInt();
				byte[] bytes = new byte[sz];
				in.readFully(bytes);

				obj.readFields(new DataInputStream(new ByteArrayInputStream(bytes)));
				this.add(obj);
			}

		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
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
		if (mList.size() > 0)
			out.writeUTF(listElementClass.getCanonicalName());
		else
			out.writeUTF(WritableComparable.class.getCanonicalName());

		for (int i = 0; i < mList.size(); i++) {
			if (mList.get(i) == null) {
				throw new IOException("Cannot serialize null fields!");
			}

			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(bytesOut);

			mList.get(i).write(dataOut);

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

	/* (non-Javadoc)
	 * @see java.util.List#add(int, java.lang.Object)
	 */
	public void add(int pos, E element) {
		
		mList.add(pos, element);
		
	}

	/* (non-Javadoc)
	 * @see java.util.List#addAll(java.util.Collection)
	 */
	public boolean addAll(Collection<? extends E> elements) {
		boolean failure = false;
		Iterator<? extends E> it = elements.iterator();
		while (it.hasNext()) {
			E obj = it.next();
			if (mList.size() == 0) 
				listElementClass = obj.getClass();
			else if (!obj.getClass().equals(listElementClass))
				throw new IllegalArgumentException("Cannot add element of type " + obj.getClass().getCanonicalName() + " to list of type " + listElementClass.getCanonicalName());
			
			if (!mList.add(obj)) failure = true;
		}
		
		
		return !failure;
	}

	/* (non-Javadoc)
	 * @see java.util.List#addAll(int, java.util.Collection)
	 */
	public boolean addAll(int pos, Collection<? extends E> elements) {
		// TODO: Check the return type of this method.
		Iterator<? extends E> it = elements.iterator();
		int curPos = pos;
		while (it.hasNext()) {
			E obj = it.next();
			if (mList.size() == 0) 
				listElementClass = obj.getClass();
			else if (!obj.getClass().equals(listElementClass))
				throw new IllegalArgumentException("Cannot add element of type " + obj.getClass().getCanonicalName() + " to list of type " + listElementClass.getCanonicalName());
			
			mList.add(curPos, obj);
			++curPos;
		}
		
		
		return true;
	}

	/* (non-Javadoc)
	 * @see java.util.List#contains(java.lang.Object)
	 */
	public boolean contains(Object element) {
		return mList.contains(element);
	}

	/* (non-Javadoc)
	 * @see java.util.List#containsAll(java.util.Collection)
	 */
	public boolean containsAll(Collection<?> elements) {
		return mList.containsAll(elements);
	}

	/* (non-Javadoc)
	 * @see java.util.List#indexOf(java.lang.Object)
	 */
	public int indexOf(Object element) {
		return mList.indexOf(element);
	}

	/* (non-Javadoc)
	 * @see java.util.List#isEmpty()
	 */
	public boolean isEmpty() {
		return mList.isEmpty();
	}

	/* (non-Javadoc)
	 * @see java.util.List#lastIndexOf(java.lang.Object)
	 */
	public int lastIndexOf(Object element) {
		return mList.lastIndexOf(element);
	}

	/* (non-Javadoc)
	 * @see java.util.List#listIterator()
	 */
	public ListIterator<E> listIterator() {
		return mList.listIterator();
	}

	/* (non-Javadoc)
	 * @see java.util.List#listIterator(int)
	 */
	public ListIterator<E> listIterator(int arg0) {
		return mList.listIterator(arg0);
	}

	/* (non-Javadoc)
	 * @see java.util.List#remove(java.lang.Object)
	 */
	public boolean remove(Object element) {
		return mList.remove(element);
	}

	/* (non-Javadoc)
	 * @see java.util.List#remove(int)
	 */
	public E remove(int pos) {
		return mList.remove(pos);
	}

	/* (non-Javadoc)
	 * @see java.util.List#removeAll(java.util.Collection)
	 */
	public boolean removeAll(Collection<?> elements) {
		return mList.removeAll(elements);
	}

	/* (non-Javadoc)
	 * @see java.util.List#retainAll(java.util.Collection)
	 */
	public boolean retainAll(Collection<?> elements) {
		return mList.retainAll(elements);
	}

	/* (non-Javadoc)
	 * @see java.util.List#subList(int, int)
	 */
	public List<E> subList(int arg0, int arg1) {
		// TODO Consider making this return a type of ListWritable rather than of ArrayList.
		return mList.subList(arg0, arg1);
	}

	/* (non-Javadoc)
	 * @see java.util.List#toArray()
	 */
	public Object[] toArray() {
		return mList.toArray();
	}

	/* (non-Javadoc)
	 * @see java.util.List#toArray(T[])
	 */
	public <T> T[] toArray(T[] arg0) {
		return mList.toArray(arg0);
	}

}
