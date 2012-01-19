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
 * Interface for a document forward index.
 *
 * @author Jimmy Lin
 *
 * @param <T> type of document
 */
public interface DocumentForwardIndex<T extends Indexable> {
  /**
   * Returns the docno, given the docid.
   *
   * @return docno for the docid 
   */
  int getDocno(String docid);

  /**
   * Returns the docid, given the docno.
   *
   * @return docid for the docno
   */
  String getDocid(int docno);

  /**
   * Fetches the document for a given docno.
   *
   * @return the document object
   */
  T getDocument(int docno);

  /**
   * Fetches the document for a given docid.
   *
   * @return the document object
   */
  T getDocument(String docid);

  /**
   * Returns the first docno in the collection.
   *
   * @return the first docno in the collection
   */
  int getFirstDocno();

  /**
   * Returns the last docno in the collection.
   *
   * @return the last docno in the collection
   */
  int getLastDocno();

  /** 
   * Loads the index.
   *
   * @param index path of the index data
   * @param mapping path of the docno/docid mapping data
   * @param fs reference to the {@code FileSystem}
   */
  void loadIndex(Path index, Path mapping, FileSystem fs) throws IOException;

  /** 
   * Returns the base path of the collection.
   *
   * @return base path of the collection
   */
  String getCollectionPath();
}
