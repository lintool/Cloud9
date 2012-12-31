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
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.util.map.HMapKI;
import edu.umd.cloud9.util.map.MapKI;

/**
 * <p>
 * Object that maps between WARC-TREC-IDs (String identifiers) to docnos (sequentially-numbered
 * ints). This object provides mappings for the Clue Web English collection; the docnos are numbered
 * from part 1 all the way through part 10.
 * </p>
 * <p>
 * Note that this class needs the data file <a href="docno.mapping"><code>docno.mapping</code></a>,
 * loaded via the {@link #loadMapping(Path, FileSystem)} method.
 * </p>
 *
 * @author Jimmy Lin
 */
public class ClueWarcDocnoMapping implements DocnoMapping {
  private static final int[] offets = new int[13217];
  private static final HMapKI<String> subdirMapping = new HMapKI<String>();

  private static final NumberFormat FormatW2 = new DecimalFormat("00");
  private static final NumberFormat FormatW5 = new DecimalFormat("00000");

  /**
   * Creates a {@code ClueWarcDocnoMapping} object
   */
  public ClueWarcDocnoMapping() {}

  @Override
  public int getDocno(String docid) {
    if (docid == null) {
      return -1;
    }

    String sec = docid.substring(10, 16);
    int secStart = subdirMapping.get(sec);

    int file = Integer.parseInt(docid.substring(17, 19));
    int cnt = Integer.parseInt(docid.substring(20, 25));

    int idx = secStart + file;
    int docno = offets[idx] + cnt;

    return docno;
  }

  @Override
  public String getDocid(int docno) {
    int i = 0;
    for (i = 0; i < offets.length; i++) {
      if (docno < offets[i]) {
        break;
      }
    }
    i--;

    String docid = null;
    for (MapKI.Entry<String> e : subdirMapping.getEntriesSortedByValue()) {
      if (e.getValue() <= i) {
        docid = "clueweb09-" + e.getKey() + "-" + FormatW2.format(i - e.getValue()) + "-"
            + FormatW5.format(docno - offets[i]);
        break;
      }
    }

    return docid;
  }

  @Override
  public void loadMapping(Path p, FileSystem fs) throws IOException {
    LineReader reader = new LineReader(fs.open(p));
    Text t = new Text();
    int cnt = 0;
    String prevSec = null;

    while (reader.readLine(t) > 0) {
      String[] arr = t.toString().split(",");

      if (prevSec == null || !arr[0].equals(prevSec)) {
        subdirMapping.put(arr[0], cnt);
      }

      offets[cnt] = Integer.parseInt(arr[3]);
      prevSec = arr[0];
      cnt++;
    }

    reader.close();
  }

  @Override
  public Builder getBuilder() {
    return new ClueWarcDocnoMappingBuilder();
  }

  /**
   * Simple program the provides access to the docno/docid mappings.
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      System.out.println("usage: (getDocno|getDocid) [mapping-file] [docid/docno]");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    System.out.println("loading mapping file " + args[1]);
    ClueWarcDocnoMapping mapping = new ClueWarcDocnoMapping();
    mapping.loadMapping(new Path(args[1]), fs);

    if (args[0].equals("getDocno")) {
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