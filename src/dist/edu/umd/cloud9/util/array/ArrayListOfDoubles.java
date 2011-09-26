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

package edu.umd.cloud9.util.array;

import java.util.Arrays;
import java.util.Iterator;
import java.util.RandomAccess;

import com.google.common.base.Preconditions;

/**
 * Object representing a list of doubles, backed by an resizable-array.
 */
public class ArrayListOfDoubles implements RandomAccess, Cloneable, Iterable<Double> {
	protected transient double[] array;
	protected int size = 0;

	private static final int INITIAL_CAPACITY_DEFAULT = 10;

	/**
	 * Constructs an empty list with the specified initial capacity.
	 *
	 * @param initialCapacity the initial capacity of the list
	 * @exception IllegalArgumentException if the specified initial capacity is negative
	 */
	public ArrayListOfDoubles(int initialCapacity) {
		if (initialCapacity < 0) {
			throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
		}

		array = new double[initialCapacity];
	}

	/**
	 * Constructs an empty list with an initial capacity of ten.
	 */
	public ArrayListOfDoubles() {
		this(INITIAL_CAPACITY_DEFAULT);
	}

  /**
   * Constructs a list from an array. Defensively makes a copy of the array.
   *
   * @param a source array
   */
	public ArrayListOfDoubles(double[] a) {
		Preconditions.checkNotNull(a);

    // Be defensive and make a copy of the array.
    array = Arrays.copyOf(a, a.length);
		size = array.length;
	}

	/**
	 * Trims the capacity of this object to be the list's current size. An
	 * application can use this operation to minimize the memory footprint of
	 * this object.
	 */
	public void trimToSize() {
		int oldCapacity = array.length;
		if (size < oldCapacity) {
			array = Arrays.copyOf(array, size);
		}
	}

	/**
	 * Increases the capacity of this object, if necessary, to ensure that it
	 * can hold at least the number of elements specified by the minimum
	 * capacity argument.
	 *
	 * @param minCapacity the desired minimum capacity
	 */
	public void ensureCapacity(int minCapacity) {
		int oldCapacity = array.length;
		if (minCapacity > oldCapacity) {
			int newCapacity = (oldCapacity * 3) / 2 + 1;
			if (newCapacity < minCapacity) {
				newCapacity = minCapacity;
			}
			array = Arrays.copyOf(array, newCapacity);
		}
	}

	/**
	 * Returns the number of elements in this list.
	 */
	public int size() {
		return size;
	}

	/**
	 * Specifies the length of this list.
	 */
	public void setSize(int sz) {
		ensureCapacity(sz);
		size = sz;
	}

	/**
	 * Returns <tt>true</tt> if this list contains no elements.
	 */
	public boolean isEmpty() {
		return size == 0;
	}

	/**
	 * Returns <tt>true</tt> if this list contains the specified element.
	 *
	 * @param n element whose presence in this list is to be tested
	 * @return <tt>true</tt> if this list contains the specified element
	 */
	public boolean contains(double n) {
		return indexOf(n) >= 0;
	}

	/**
	 * Returns the index of the first occurrence of the specified element in
	 * this list, or -1 if this list does not contain the element.
	 */
	public int indexOf(double n) {
		for (int i = 0; i < size; i++) {
			if (n == array[i]) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * Returns the index of the last occurrence of the specified element in this
	 * list, or -1 if this list does not contain the element.
	 */
	public int lastIndexOf(double n) {
		for (int i = size - 1; i >= 0; i--) {
			if (n == array[i]) {
				return i;
			}
		}
		return -1;
	}

	/**
	 * Returns a clone of this object.
	 */
	public ArrayListOfDoubles clone() {
		return new ArrayListOfDoubles(Arrays.copyOf(array, this.size()));
	}

	/**
	 * Returns the element at the specified position in this list.
	 *
	 * @param index index of the element to return
	 * @return the element at the specified position in this list
	 */
	public double get(int index) {
		return array[index];
	}

	/**
	 * Replaces the element at the specified position in this list with the
	 * specified element.
	 *
	 * @param index index of the element to replace
	 * @param element element to be stored at the specified position
	 * @return the element previously at the specified position
	 */
	public double set(int index, double element) {
		double oldValue = array[index];
		array[index] = element;
		return oldValue;
	}

	/**
	 * Appends the specified element to the end of this list.
	 */
	public ArrayListOfDoubles add(double e) {
		ensureCapacity(size + 1); // Increments modCount!!
		array[size++] = e;
		return this;
	}

	/**
	 * Inserts the specified element at the specified position in this list.
	 * Shifts the element currently at that position (if any) and any subsequent
	 * elements to the right (adds one to their indices).
	 *
	 * @param index index at which the specified element is to be inserted
	 * @param element element to be inserted
	 */
	public ArrayListOfDoubles add(int index, double element) {
		if (index > size || index < 0) {
			throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
		}

		ensureCapacity(size + 1); // Increments modCount!!
		System.arraycopy(array, index, array, index + 1, size - index);
		array[index] = element;
		size++;
		return this;
	}

	/**
	 * Removes the element at the specified position in this list. Shifts any
	 * subsequent elements to the left (subtracts one from their indices).
	 *
	 * @param index the index of the element to be removed
	 * @return the element that was removed from the list
	 */
	public double remove(int index) {
	  if(index >= size){
      throw new ArrayIndexOutOfBoundsException();
    }
	  double oldValue = array[index];

		int numMoved = size - index - 1;
		if (numMoved > 0) {
			System.arraycopy(array, index + 1, array, index, numMoved);
		}

		size--;
		return oldValue;
	}

	/**
	 * Removes all of the elements from this list. The list will be empty after
	 * this call returns.
	 */
	public void clear() {
		size = 0;
		array = new double[INITIAL_CAPACITY_DEFAULT];
	}

	/**
	 * Returns the array backing this object. Note that this array may be longer
	 * than the number of elements in the list.
	 *
	 * @return array backing this object
	 */
	public double[] getArray() {
		return array;
	}

	/**
	 * Returns an iterator for this list. Note that this method is included only
	 * for convenience to conform to the <code>Iterable</code> interface; this
	 * method is not efficient because of autoboxing.
	 */
	public Iterator<Double> iterator() {
		return new Iterator<Double>() {
			int cnt = 0;
			public boolean hasNext() { return cnt < size(); }
			public void remove() { throw new UnsupportedOperationException(); }
			public Double next() { return get(cnt++); }
		};
	}

	/**
	 * Returns a string representation of the object, explicitly printing out the
	 * first <i>n</i> elements of this list.
	 */
	public String toString(int n) {
    StringBuilder s = new StringBuilder();

    s.append("[");
    int sz = size() > n ? n : size;

    for (int i = 0; i < sz; i++) {
      s.append(get(i));
      if (i < sz - 1) {
        s.append(", ");
      }
    }

    s.append(size() > n ? String.format(" ... (%d more) ]", size() - n) : "]");

    return s.toString();
	}

	@Override
	public String toString() {
		return toString(10);
	}

  /**
   * Sorts this list.
   */
  public void sort() {
    trimToSize();
    Arrays.sort(getArray());
  }
}
