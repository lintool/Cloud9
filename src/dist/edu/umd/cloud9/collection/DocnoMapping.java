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

package edu.umd.cloud9.collection;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * <p>
 * Interface for an object that maintains the mapping between docids and docnos.
 * A docid is a globally-unique String identifier for a document in the
 * collection. For many types of information retrieval algorithms, documents in
 * the collection must be sequentially numbered; thus, each document in the
 * collection must be assigned a unique integer identifier, which is its docno.
 * Typically, the docid to docno mappings are stored in a mappings file, which
 * is loaded into memory by concrete objects implementing this interface.
 * </p>
 * 
 * <p>
 * Unless there are compelling reasons otherwise, it is preferable to start
 * numbering docnos from one instead of zero. This is because zero cannot be
 * represented in many common compression schemes that are used in information
 * retrieval (e.g., Golomb codes).
 * </p>
 */
public interface DocnoMapping {

	/**
	 * Returns the docno for a particular docid.
	 * 
	 * @param docid
	 *            the docid
	 * @return the docno for the docid
	 */
	public int getDocno(String docid);

	/**
	 * Returns the docid for a particular docno.
	 * 
	 * @param docno
	 *            the docno
	 * @return the docid for the docno
	 */
	public String getDocid(int docno);

	/**
	 * Loads a mapping file containing the docid to docno mappings.
	 * 
	 * @param p
	 *            path to the mappings file
	 * @param fs
	 *            appropriate FileSystem
	 * @throws IOException
	 */
	public void loadMapping(Path p, FileSystem fs) throws IOException;
}
