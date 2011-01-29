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

import edu.umd.cloud9.util.array.ArrayListOfInts;

/**
 * @author Tamer
 *
 */
public class StackOfInts extends ArrayListOfInts {
	
	public void push(int i){
		ensureCapacity(size + 1); // Increments modCount!!
		array[size++] = i;
	}
	
	public int pop(){
		int value = array[size-1];
		size--;
		return value;
	}
}
