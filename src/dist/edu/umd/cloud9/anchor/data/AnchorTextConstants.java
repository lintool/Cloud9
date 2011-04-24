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

package edu.umd.cloud9.anchor.data;

/**
 * <p>
 * This interface holds the constants used in AnchorText data structure.
 * </p>
 * 
 * @author Nima Asadi
 * 
 */

public interface AnchorTextConstants {
	
	public static final String EMPTY_STRING = "";
		
	/**
	 * Indicates an Internal Incoming Link
	 */
	public static final byte INTERNAL_IN_LINK = 0;
	
	/**
	 * Indicates an External Incoming Link
	 */
	public static final byte EXTERNAL_IN_LINK = 1;
	
	/**
	 * Indicates an Internal Outgoing Link
	 */
	public static final byte INTERNAL_OUT_LINK = 2;
	
	/**
	 * Indicates an External Outgoing Link
	 */
	public static final byte EXTERNAL_OUT_LINK = 3;
	
	
	/**
	 * Indicates whether the data structure is only used to hold a URL address
	 */
	public static final byte URL_FIELD = 4;
	
	/**
	 * Indicates whether the data structure is only used to hold a document number/id
	 */
	public static final byte DOCNO_FIELD = 5;
	
	/**
	 * Indicates whether the data structure is only used to hold the outdegree for a document
	 */
	public static final byte OUT_DEGREE = 6;
	

	/**
	 * Indicates whether the data structure is only used to hold the indegree for a document
	 */
	public static final byte IN_DEGREE = 7;
	

	/**
	 * Indicates whether the data structure is only used to hold other types of information
	 */
	public static final byte OTHER_TYPES = 8;	
	
	

	/**
	 * Indicates a weighted External Incoming Link
	 */
	public static final byte WEIGHTED_EXTERNAL_IN_LINK = 11;
		
	

	/**
	 * Shows the maximum number of sources/targets an AnchorText object can hold. 
	 * If the number of sources/targets is greater or equal to this value, the 
	 * AnchorText object is split into several smaller objects in order to fulfill 
	 * this requirement. 
	 */
	public static final int MAXIMUM_SOURCES_PER_PACKET = 1*1024*1024;
				
}
