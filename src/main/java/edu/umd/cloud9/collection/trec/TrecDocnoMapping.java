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
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import edu.umd.cloud9.collection.DocnoMapping;

/**
 * Object that maps between TREC docids (String identifiers) to docnos (sequentially-numbered 
 * ints). The {@code main} of this class provides a simple program for accessing docno mappings.
 * Command-line arguments are as follows:
 *
 * <ul>
 * <li>list, getDocno, getDocid: the command&mdash;list all mappings; get docno from docid; or, get
 * docid from docno</li>
 * <li>[mappings-file]: the mappings file</li>
 * <li>[docid/docno]: the docid or docno (optional)</li>
 * </ul>
 *
 * @author Jimmy Lin
 */
public class TrecDocnoMapping implements DocnoMapping {
  private static final Logger LOG = Logger.getLogger(TrecDocnoMapping.class);

  private String[] docids;

  /**
   * Creates a {@code TrecDocnoMapping} object.
   */
  public TrecDocnoMapping() {}

  @Override
  public int getDocno(String docid) {
    Preconditions.checkNotNull(docid);
    return Arrays.binarySearch(docids, docid);
  }

  @Override
  public String getDocid(int docno) {
    return docids[docno];
  }

  @Override
  public void loadMapping(Path p, FileSystem fs) throws IOException {
    Preconditions.checkNotNull(p);
    Preconditions.checkNotNull(fs);
    docids = TrecDocnoMapping.readMappingData(p, fs);
  }

  /**
   * Creates a mappings file from the contents of a flat text file containing docid to docno
   * mappings. This method is used by {@link TrecDocnoMappingBuilder} internally.
   *
   * @param input flat text file containing docid to docno mappings
   * @param output output mappings data file
   * @param fs {@code FileSystem} to write to
   * @throws IOException
   */
  static public void writeMappingData(Path input, Path output, FileSystem fs) throws IOException {
    Preconditions.checkNotNull(input);
    Preconditions.checkNotNull(output);
    Preconditions.checkNotNull(fs);

    LOG.info("Writing docno data to " + output);
    LineReader reader = new LineReader(fs.open(input));
    List<String> list = Lists.newArrayList();

    LOG.info("Reading " + input);
    int cnt = 0;
    Text line = new Text();
    while (reader.readLine(line) > 0) {
      String[] arr = line.toString().split("\\t");
      list.add(arr[0]);
      cnt++;
      if (cnt % 100000 == 0) {
        LOG.info(cnt + " docs");
      }
    }
    reader.close();
    LOG.info(cnt + " docs total. Done!");

    cnt = 0;
    LOG.info("Writing " + output);
    FSDataOutputStream out = fs.create(output, true);
    out.writeInt(list.size());
    for (int i = 0; i < list.size(); i++) {
      out.writeUTF(list.get(i));
      cnt++;
      if (cnt % 100000 == 0) {
        LOG.info(cnt + " docs");
      }
    }
    out.close();
    LOG.info(cnt + " docs total. Done!");
  }

  /**
   * Reads a mappings file into memory.
   *
   * @param p path to the mappings data file
   * @param fs appropriate FileSystem
   * @return an array of docids; the index position of each docid is its docno
   * @throws IOException
   */
  static public String[] readMappingData(Path p, FileSystem fs) throws IOException {
    Preconditions.checkNotNull(p);
    Preconditions.checkNotNull(fs);

    FSDataInputStream in = fs.open(p);

    // The docnos start at one, so we need an array that's one larger than number of docs.
    int sz = in.readInt() + 1;
    String[] arr = new String[sz];

    for (int i = 1; i < sz; i++) {
      arr[i] = in.readUTF();
    }
    in.close();

    // We can't leave the zero'th entry null, or else we might get a null pointer exception during a
    // binary search on the array.
    arr[0] = "";

    return arr;
  }

  @Override
  public Builder getBuilder() {
    return new TrecDocnoMappingBuilder();
  }

  /**
   * Simple program the provides access to the docno/docid mappings.
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("usage: (list|getDocno|getDocid) [mapping-file] [docid/docno]");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    System.out.println("loading mapping file " + args[1]);
    TrecDocnoMapping mapping = new TrecDocnoMapping();
    mapping.loadMapping(new Path(args[1]), fs);

    if (args[0].equals("list")) {
      for (int i = 1; i < mapping.docids.length; i++) {
        System.out.println(i + "\t" + mapping.docids[i]);
      }
    } else if (args[0].equals("getDocno")) {
      System.out.println("looking up docno for \"" + args[2] + "\"");
      int idx = mapping.getDocno(args[2]);
      if (idx > 0) {
        System.out.println(mapping.getDocno(args[2]));
      } else {
        System.err.print("Invalid docid!");
      }
    } else if (args[0].equals("getDocid")) {
      try {
        System.out.println("looking up docid for " + args[2]);
        System.out.println(mapping.getDocid(Integer.parseInt(args[2])));
      } catch (Exception e) {
        System.err.print("Invalid docno!");
      }
    } else {
      System.out.println("Invalid command!");
      System.out.println("usage: (list|getDocno|getDocid) [mappings-file] [docid/docno]");
    }
  }
}
