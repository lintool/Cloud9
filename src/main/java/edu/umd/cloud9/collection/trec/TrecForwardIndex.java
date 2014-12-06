/*
 * Cloud9: A Hadoop toolkit for working with big data
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

import com.google.common.base.Preconditions;

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
    Preconditions.checkNotNull(docid);
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
      LOG.info(String.format("docno %d: byte offset %d, length %d",
          docno, offsets[docno], lengths[docno]));

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
  public void loadIndex(Path index, Path mapping, FileSystem fs) throws IOException {
    FSDataInputStream in = fs.open(index);

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
    docnoMapping.loadMapping(mapping, fs);
  }

  /**
   * Simple program the provides access to the document contents.
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 4) {
      System.out.println("usage: [findex] [mapping-file] [getDocno|getDocid] [docid/docno]");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    System.out.println("forward index: " + args[0]);
    System.out.println("mapping file: " + args[1]);

    TrecForwardIndex findex = new TrecForwardIndex();
    findex.loadIndex(new Path(args[0]), new Path(args[1]), fs);

    if (args[2].equals("getDocno")) {
      System.out.println("looking up docno " + args[3]);
      System.out.println(findex.getDocument(Integer.parseInt(args[3])).getDisplayContent());
    } else if (args[2].equals("getDocid")) {
      System.out.println("looking up docid " + args[3]);
      System.out.println(findex.getDocument(args[3]).getDisplayContent());
    } else {
      System.out.println("Invalid command!");
      System.out.println("usage: [findex] [mapping-file] [getDocno|getDocid] [docid/docno]");
    }
  }
}
