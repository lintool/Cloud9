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

package edu.umd.cloud9.collection.aquaint2;

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

public class Aquaint2DocnoMapping implements DocnoMapping {
  private static final Logger LOG = Logger.getLogger(Aquaint2DocnoMapping.class);

  private String[] docids;

  @Override
  public int getDocno(String docid) {
    Preconditions.checkNotNull(docid);
    return Arrays.binarySearch(docids, docid);
  }

  @Override
  public String getDocid(int docno) {
    Preconditions.checkArgument(docno > 0);
    return docids[docno];
  }

  @Override
  public void loadMapping(Path p, FileSystem fs) throws IOException {
    docids = Aquaint2DocnoMapping.readDocnoData(p, fs);
  }

  static public void writeDocnoData(Path input, Path output, FileSystem fs) throws IOException {
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

  static public String[] readDocnoData(Path p, FileSystem fs) throws IOException {
    LOG.warn("p: " + p);
    FSDataInputStream in = fs.open(p);

    // Docnos start at one, so we need an array that's one larger than number of docs.
    int sz = in.readInt() + 1;
    LOG.warn("creating array of length: " + sz);
    String[] arr = new String[sz];

    for (int i = 1; i < sz; i++) {
      arr[i] = in.readUTF();
    }
    in.close();

    // Can't leave the zero'th entry null, or else we might get a null pointer exception during a
    // binary search on the array.
    arr[0] = "";

    return arr;
  }

  @Override
  public Builder getBuilder() {
    throw new UnsupportedOperationException();
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("usage: (list|getDocno|getDocid) [mapping-file] [docid/docno]");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    System.out.println("loading mapping file " + args[1]);
    Aquaint2DocnoMapping mapping = new Aquaint2DocnoMapping();
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
      System.out.println("usage: (list|getDocno|getDocid) [mapping-file] [docid/docno]");
    }
  }
}