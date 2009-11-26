/**
 * 
 */
package edu.umd.cloud9.util;

/**
 * @author Tamer
 *
 */
public class StackOfInts extends ArrayListOfInts {
	
	public void push(int i){
		ensureCapacity(size + 1); // Increments modCount!!
		mArray[size++] = i;
	}
	
	public int pop(){
		int value = mArray[size-1];
		size--;
		return value;
	}
}
