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

package edu.umd.cloud9.io.array;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * 	An array of floats that implements Writable class.
 * 
 * @author ferhanture
 */
public class ArrayOfFloatsWritable implements Writable {
	float[] array;
	
	
	/**
	 * 	Constructor with no arguments.
	 */
	public ArrayOfFloatsWritable() {
		super();
	}

	
	/**
	 * Constructor that takes the size of the array as an argument.
	 * @param size
	 * 		number of floats in array
	 */
	public ArrayOfFloatsWritable(int size) {
		super();
		array = new float[size];
	}

	public void readFields(DataInput in) throws IOException {
		int size = in.readInt();
		array = new float[size];
		for(int i=0;i<size;i++){
			set(i, in.readFloat());
		}
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(size());
		for(int i=0;i<size();i++){
			out.writeFloat(get(i));
		}
	}
	
	/**
	 * Returns the float value at position <i>i</i>.
	 * 
	 * @param i
	 * 		index of float to be returned
	 * @return
	 * 		float value at position <i>i</i>
	 */
	public float get(int i){
		return array[i];
	}
	
	
	/**
	 * Sets the float at position <i>i</i> to <i>f</i>.
	 * @param i
	 * 		position in array
	 * @param f
	 * 		float value to be set
	 */
	public void set(int i, float f){
		array[i] = f;
	}

	
	/**
	 * Returns the size of the float array.
	 * @return
	 * 		size of array
	 */
	public int size() {
		return array.length;
	}
	
	public String toString(){
		String s = "[";
		for(int i=0;i<size();i++){
			s+=get(i)+",";
		}
		s+="]";
		return s;
	}
}
