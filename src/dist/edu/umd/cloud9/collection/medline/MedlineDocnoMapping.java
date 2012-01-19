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

package edu.umd.cloud9.collection.medline;

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
 * <p>
 * Object that maps between MEDLINE docids (PMIDs) to docnos (sequentially-numbered ints).
 * </p>
 *
 * <p>
 * The <code>main</code> of this class provides a simple program for accessing docno mappings.
 * Command-line arguments are as follows:
 * </p>
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
public class MedlineDocnoMapping implements DocnoMapping {
  private static final Logger LOG = Logger.getLogger(MedlineDocnoMapping.class);

  private int[] pmids;

  /**
   * Creates a <code>MedlineDocnoMapping</code> object
   */
  public MedlineDocnoMapping() {
  }

  public int getDocno(String docid) {
    // docnos are numbered starting from one
    return Arrays.binarySearch(pmids, Integer.parseInt(docid));
  }

  public String getDocid(int docno) {
    // docnos are numbered starting from one
    return new Integer(pmids[docno]).toString();
  }

  public void loadMapping(Path p, FileSystem fs) throws IOException {
    pmids = MedlineDocnoMapping.readDocidData(p, fs);
  }

  /**
   * Creates a mappings file from the contents of a flat text file containing docid to docno
   * mappings. This method is used by {@link NumberMedlineCitations} internally.
   *
   * @param input flat text file containing docid to docno mappings
   * @param output output mappings file
   * @param fs reference to the file system
   * @throws IOException
   */
  static public void writeMappingData(Path input, Path output, FileSystem fs)
      throws IOException {
    Preconditions.checkNotNull(input);
    Preconditions.checkNotNull(output);
    Preconditions.checkNotNull(fs);

    LOG.info("Writing docids to " + output);
    LineReader reader = new LineReader(fs.open(input));
    List<Integer> list = Lists.newArrayList();

    LOG.info("Reading " + input);
    int cnt = 0;
    Text line = new Text();
    while (reader.readLine(line) > 0) {
      String[] arr = line.toString().split("\\t");
      list.add(Integer.parseInt(arr[0]));
      cnt++;
      if (cnt % 500000 == 0) {
        LOG.info(cnt);
      }
    }
    reader.close();
    LOG.info("Done! Total of " + cnt + " docids read.");

    cnt = 0;
    LOG.info("Writing " + output);
    FSDataOutputStream out = fs.create(output, true);
    out.writeInt(list.size());
    for (int i = 0; i < list.size(); i++) {
      out.writeInt(list.get(i));
      cnt++;
      if (cnt % 500000 == 0) {
        LOG.info(cnt);
      }
    }
    out.close();
    LOG.info("Done! Total of " + cnt + " docids written.");
  }

  /**
   * Reads a mappings file into memory.
   *
   * @param p path to the mappings file
   * @param fs appropriate FileSystem
   * @return an array of docids; the index position of each docid is its docno
   * @throws IOException
   */
  static public int[] readDocidData(Path p, FileSystem fs) throws IOException {
    LOG.info("Reading docid mapping...");

    FSDataInputStream in = fs.open(p);
    // docnos start at one, so we need an array that's one larger than
    // number of docs
    int sz = in.readInt() + 1;
    int[] arr = new int[sz];
    int cnt = 0;
    for (int i = 1; i < sz; i++) {
      arr[i] = in.readInt();

      if (i % 500000 == 0) {
        LOG.info(i);
      }
      cnt++;
    }
    in.close();

    LOG.info(cnt + " docid mappings read");
    return arr;
  }

  /**
   * Simple program the provides access to the docno/docid mappings.
   *
   * @param args command-line arguments
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("usage: (list|getDocno|getDocid) [mapping-file] [docid/docno]");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    System.out.println("loading mapping file " + args[1]);
    MedlineDocnoMapping mapping = new MedlineDocnoMapping();
    mapping.loadMapping(new Path(args[1]), fs);

    if (args[0].equals("list")) {
      for (int i = 1; i < mapping.pmids.length; i++) {
        System.out.println(i + "\t" + mapping.pmids[i]);
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
