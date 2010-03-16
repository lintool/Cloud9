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

package edu.umd.cloud9.util;

import java.util.Arrays;
import java.util.RandomAccess;

/**
 * Object representing a list of floats, backed by an resizable-array.
 */
public class ArrayListOfFloats implements RandomAccess, Cloneable {
	protected transient float[] mArray;
	protected int size = 0;

	/**
	 * Constructs an empty list with the specified initial capacity.
	 * 
	 * @param initialCapacity
	 *            the initial capacity of the list
	 * @exception IllegalArgumentException
	 *                if the specified initial capacity is negative
	 */
	public ArrayListOfFloats(int initialCapacity) {
		if (initialCapacity < 0)
			throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
		this.mArray = new float[initialCapacity];
	}

	/**
	 * Constructs an empty list with an initial capacity of ten.
	 */
	public ArrayListOfFloats() {
		this(10);
	}

	public ArrayListOfFloats(float[] a) {
		mArray = a;
		size = mArray.length;
	}

	/**
	 * Trims the capacity of this object to be the list's current size. An
	 * application can use this operation to minimize the memory footprint of
	 * this object.
	 */
	public void trimToSize() {
		int oldCapacity = mArray.length;
		if (size < oldCapacity) {
			mArray = Arrays.copyOf(mArray, size);
		}
	}

	/**
	 * Increases the capacity of this object, if necessary, to ensure that it
	 * can hold at least the number of elements specified by the minimum
	 * capacity argument.
	 * 
	 * @param minCapacity
	 *            the desired minimum capacity
	 */
	public void ensureCapacity(int minCapacity) {
		int oldCapacity = mArray.length;
		if (minCapacity > oldCapacity) {
			int newCapacity = (oldCapacity * 3) / 2 + 1;
			if (newCapacity < minCapacity)
				newCapacity = minCapacity;
			mArray = Arrays.copyOf(mArray, newCapacity);
		}
	}

	/**
	 * Returns the number of elements in this list.
	 * 
	 * @return the number of elements in this list
	 */
	public int size() {
		return size;
	}

	/**
	 * Returns <tt>true</tt> if this list contains no elements.
	 * 
	 * @return <tt>true</tt> if this list contains no elements
	 */
	public boolean isEmpty() {
		return size == 0;
	}

	/**
	 * Returns <tt>true</tt> if this list contains the specified element.
	 * 
	 * @param n
	 *            element whose presence in this list is to be tested
	 * @return <tt>true</tt> if this list contains the specified element
	 */
	public boolean contains(float n) {
		return indexOf(n) >= 0;
	}

	/**
	 * Returns the index of the first occurrence of the specified element in
	 * this list, or -1 if this list does not contain the element.
	 */
	public int indexOf(float n) {
		for (int i = 0; i < size; i++)
			if (n == mArray[i])
				return i;
		return -1;
	}

	/**
	 * Returns the index of the last occurrence of the specified element in this
	 * list, or -1 if this list does not contain the element.
	 */
	public int lastIndexOf(float n) {
		for (int i = size - 1; i >= 0; i--)
			if (n == mArray[i])
				return i;
		return -1;
	}

	/**
	 * Returns a clone of this object.
	 * 
	 * @return a clone of this object
	 */
	public ArrayListOfFloats clone() {
		return new ArrayListOfFloats(Arrays.copyOf(mArray, this.size()));
	}

	/**
	 * Returns the element at the specified position in this list.
	 * 
	 * @param index
	 *            index of the element to return
	 * @return the element at the specified position in this list
	 */
	public float get(int index) {
		return mArray[index];
	}

	/**
	 * Replaces the element at the specified position in this list with the
	 * specified element.
	 * 
	 * @param index
	 *            index of the element to replace
	 * @param element
	 *            element to be stored at the specified position
	 * @return the element previously at the specified position
	 */
	public float set(int index, float element) {
		float oldValue = mArray[index];
		mArray[index] = element;
		return oldValue;
	}

	/**
	 * Appends the specified element to the end of this list.
	 * 
	 * @param e
	 *            element to be appended to this list
	 */
	public void add(float e) {
		ensureCapacity(size + 1); // Increments modCount!!
		mArray[size++] = e;
	}

	/**
	 * Inserts the specified element at the specified position in this list.
	 * Shifts the element currently at that position (if any) and any subsequent
	 * elements to the right (adds one to their indices).
	 * 
	 * @param index
	 *            index at which the specified element is to be inserted
	 * @param element
	 *            element to be inserted
	 */
	public void add(int index, float element) {
		if (index > size || index < 0)
			throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);

		ensureCapacity(size + 1); // Increments modCount!!
		System.arraycopy(mArray, index, mArray, index + 1, size - index);
		mArray[index] = element;
		size++;
	}

	/**
	 * Removes the element at the specified position in this list. Shifts any
	 * subsequent elements to the left (subtracts one from their indices).
	 * 
	 * @param index
	 *            the index of the element to be removed
	 * @return the element that was removed from the list
	 */
	public float remove(int index) {
		float oldValue = mArray[index];

		int numMoved = size - index - 1;
		if (numMoved > 0)
			System.arraycopy(mArray, index + 1, mArray, index, numMoved);

		return oldValue;
	}

	/**
	 * Removes all of the elements from this list. The list will be empty after
	 * this call returns.
	 */
	public void clear() {
		size = 0;
	}

	/**
	 * Returns the array backing this object. Note that this array may be longer
	 * than the number of elements in the list.
	 * 
	 * @return array backing this object
	 */
	public float[] getArray() {
		return mArray;
	}
}
