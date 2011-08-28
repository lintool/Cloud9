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

package edu.umd.cloud9.collection.trec;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocumentForwardIndex;

/**
 * A document forward index for TREC collections.
 *
 * @author Jimmy Lin
 */
public class TrecForwardIndex implements DocumentForwardIndex<TrecDocument> {
  private static final Logger LOG = Logger.getLogger(TrecForwardIndex.class);

  private long[] offsets;
  private int[] lengths;
  private FSDataInputStream input;
  private TrecDocnoMapping docnoMapping = new TrecDocnoMapping();
  private String path;

  @Override
  public int getDocno(String docid) {
    return docnoMapping.getDocno(docid);
  }

  @Override
  public String getDocid(int docno) {
    return docnoMapping.getDocid(docno);
  }

  @Override
  public int getLastDocno() {
    return offsets.length - 1;
  }

  @Override
  public int getFirstDocno() {
    return 1;
  }

  @Override
  public String getCollectionPath() {
    return path;
  }

  @Override
  public TrecDocument getDocument(String docid) {
    return getDocument(docnoMapping.getDocno(docid));
  }

  @Override
  public TrecDocument getDocument(int docno) {
    TrecDocument doc = new TrecDocument();

    try {
      LOG.info("docno " + docno + ": byte offset " + offsets[docno] + ", length "
          + lengths[docno]);

      input.seek(offsets[docno]);

      byte[] arr = new byte[lengths[docno]];

      input.read(arr);

      TrecDocument.readDocument(doc, new String(arr));
    } catch (IOException e) {
      e.printStackTrace();
    }

    return doc;
  }

  @Override
  public void loadIndex(String indexFile, String mappingDataFile) throws IOException {
    Path p = new Path(indexFile);
    FileSystem fs = FileSystem.get(new Configuration());
    FSDataInputStream in = fs.open(p);

    // Read and throw away.
    in.readUTF();
    path = in.readUTF();

    // Docnos start at one, so we need an array that's one larger than number of docs.
    int sz = in.readInt() + 1;
    offsets = new long[sz];
    lengths = new int[sz];

    for (int i = 1; i < sz; i++) {
      offsets[i] = in.readLong();
      lengths[i] = in.readInt();
    }
    in.close();

    input = fs.open(new Path(path));
    docnoMapping.loadMapping(new Path(mappingDataFile), fs);
  }
}
