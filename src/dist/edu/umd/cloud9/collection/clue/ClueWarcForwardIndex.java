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

package edu.umd.cloud9.collection.clue;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.DocumentForwardIndex;

public class ClueWarcForwardIndex implements DocumentForwardIndex<ClueWarcRecord> {
  private static final Logger LOG = Logger.getLogger(ClueWarcForwardIndex.class);
  private static DecimalFormat FORMAT5 = new DecimalFormat("00000");

  private Configuration conf;

  private int[] docnos;
  private int[] offsets;
  private short[] fileno;
  private String collectionPath;
  private int lastDocno = -1;

  private ClueWarcDocnoMapping docnoMapping = new ClueWarcDocnoMapping();

  public ClueWarcForwardIndex() {}

  @Override
  public void loadIndex(Path index, Path mapping, FileSystem fs) throws IOException {
    this.conf = fs.getConf();

    LOG.info("Loading forward index: " + index);
    docnoMapping.loadMapping(mapping, fs);

    FSDataInputStream in = fs.open(index);

    // Class name; throw away.
    in.readUTF();
    collectionPath = in.readUTF();

    int blocks = in.readInt();

    LOG.info(blocks + " blocks expected");
    docnos = new int[blocks];
    offsets = new int[blocks];
    fileno = new short[blocks];

    for (int i = 0; i < blocks; i++) {
      docnos[i] = in.readInt();
      offsets[i] = in.readInt();
      fileno[i] = in.readShort();

      if (i > 0 && i % 100000 == 0)
        LOG.info(i + " blocks read");
    }

    in.close();
  }

  @Override
  public String getCollectionPath() {
    return collectionPath;
  }

  @Override
  public ClueWarcRecord getDocument(int docno) {
    long start = System.currentTimeMillis();

    // Trap invalid docnos.
    if (docno < getFirstDocno() || docno > getLastDocno()) {
      return null;
    }

    int idx = Arrays.binarySearch(docnos, docno);

    if (idx < 0) {
      idx = -idx - 2;
    }

    DecimalFormat df = new DecimalFormat("00000");
    String file = collectionPath + "/part-" + df.format(fileno[idx]);

    LOG.info("fetching docno " + docno + ": seeking to " + offsets[idx] + " at " + file);

    try {
      SequenceFile.Reader reader =
          new SequenceFile.Reader(conf, SequenceFile.Reader.file(new Path(file)));

      IntWritable key = new IntWritable();
      ClueWarcRecord value = new ClueWarcRecord();

      reader.seek(offsets[idx]);

      while (reader.next(key)) {
        if (key.get() == docno) {
          break;
        }
      }

      reader.getCurrentValue(value);
      reader.close();

      long duration = System.currentTimeMillis() - start;

      LOG.info(" docno " + docno + " fetched in " + duration + "ms");
      return value;
    } catch (IOException e) {
      e.printStackTrace();
    }

    return null;
  }

  @Override
  public ClueWarcRecord getDocument(String docid) {
    return getDocument(docnoMapping.getDocno(docid));
  }

  @Override
  public int getDocno(String docid) {
    return docnoMapping.getDocno(docid);
  }

  @Override
  public String getDocid(int docno) {
    return docnoMapping.getDocid(docno);
  }

  @Override
  public int getFirstDocno() {
    return docnos[0];
  }

  @Override
  public int getLastDocno() {
    if (lastDocno != -1) {
      return lastDocno;
    }

    // Find the last entry, and then see all the way to the end of the collection.
    int idx = docnos.length - 1;

    String file = collectionPath + "/part-" + FORMAT5.format(fileno[idx]);

    try {
      SequenceFile.Reader reader = new SequenceFile.Reader(conf,
          SequenceFile.Reader.file(new Path(file)));
      IntWritable key = new IntWritable();

      reader.seek(offsets[idx]);

      while (reader.next(key));
      lastDocno = key.get();
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return lastDocno;
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

    ClueWarcForwardIndex findex = new ClueWarcForwardIndex();
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
