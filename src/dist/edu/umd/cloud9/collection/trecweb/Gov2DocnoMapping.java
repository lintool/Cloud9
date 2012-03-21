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

package edu.umd.cloud9.collection.trecweb;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.collection.DocnoMapping;

public class Gov2DocnoMapping implements DocnoMapping {
  private static final NumberFormat FormatW2 = new DecimalFormat("00");
  private static final NumberFormat FormatW3 = new DecimalFormat("000");
  private static final NumberFormat FormatW7 = new DecimalFormat("0000000");
  private static final NumberFormat FormatW8 = new DecimalFormat("00000000");

  private int[][] docids;
  private int[] offsets;

  public Gov2DocnoMapping() {}

  @Override
  public int getDocno(String docid) {
    Preconditions.checkNotNull(docid);

    int dirNum = Integer.parseInt(docid.substring(2, 5));
    int subdirNum = Integer.parseInt(docid.substring(6, 8));
    int num = Integer.parseInt(docid.substring(9));
    int offset = Arrays.binarySearch(docids[dirNum * 100 + subdirNum], num);

    return offsets[dirNum * 100 + subdirNum] + offset + 1;
  }

  @Override
  public String getDocid(int docno) {
    docno--;

    int i = 0;
    for (i = 0; i < docids.length; i++) {
      if (docno < offsets[i]) {
        break;
      }
    }
    i--;
    while (offsets[i] == -1) {
      i--;
    }

    int subdirNum = i % 100;
    int dirNum = (i - subdirNum) / 100;
    int num = docids[i][docno - offsets[i]];

    if (num >= 10000000) {
      return "GX" + FormatW3.format(dirNum) + "-" + FormatW2.format(subdirNum) + "-"
          + FormatW8.format(num);
    }

    return "GX" + FormatW3.format(dirNum) + "-" + FormatW2.format(subdirNum) + "-"
        + FormatW7.format(num);
  }

  @Override
  public void loadMapping(Path p, FileSystem fs) throws IOException {
    FSDataInputStream in = fs.open(p);

    List<Integer> ids = null;
    int lastOffset = -1;

    int sz = in.readInt();
    docids = new int[273 * 100][];
    offsets = new int[273 * 100];

    for (int i = 0; i < 273 * 100; i++) {
      offsets[i] = -1;
    }

    for (int i = 0; i < sz; i++) {
      String docName = in.readUTF();

      // GX243-38-13543987
      int dirNum = Integer.parseInt(docName.substring(2, 5));
      int subdirNum = Integer.parseInt(docName.substring(6, 8));
      int num = Integer.parseInt(docName.substring(9));

      int curOffset = dirNum * 100 + subdirNum;

      if (curOffset != lastOffset) {
        if (ids != null) {
          int[] idArray = new int[ids.size()];
          for (int j = 0; j < ids.size(); j++) {
            idArray[j] = ids.get(j);
          }
          Arrays.sort(idArray);
          docids[lastOffset] = idArray;
        }
        lastOffset = curOffset;
        ids = new ArrayList<Integer>();
        offsets[curOffset] = i;
      }
      ids.add(num);
    }

    if (ids != null) {
      int[] idArray = new int[ids.size()];
      for (int j = 0; j < ids.size(); j++) {
        idArray[j] = ids.get(j);
      }
      Arrays.sort(idArray);
      docids[lastOffset] = idArray;
    }

    in.close();
  }


  @Override
  public Builder getBuilder() {
    return new TrecWebDocnoMappingBuilder();
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
    Gov2DocnoMapping mapping = new Gov2DocnoMapping();
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
      System.out.println("usage: (list|getDocno|getDocid) [mapping-file] [docid/docno]");
    }
  }
}
