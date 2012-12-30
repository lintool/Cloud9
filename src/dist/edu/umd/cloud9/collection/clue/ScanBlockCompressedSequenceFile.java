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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import com.google.common.collect.Lists;

public class ScanBlockCompressedSequenceFile {

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.out.println("usage: [SequenceFile]");
      System.exit(-1);
    }

    List<Long> seekPoints = Lists.newArrayList();
    long pos = -1;
    long prevPos = -1;

    int prevDocno = 0;

    Path path = new Path(args[0]);
    Configuration config = new Configuration();
    SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(path));

    IntWritable key = new IntWritable();
    ClueWarcRecord value = new ClueWarcRecord();

    pos = reader.getPosition();
    int cnt = 0;
    while (reader.next(key, value)) {
      if (prevPos != -1 && prevPos != pos) {
        System.out.println("## beginning of block at " + prevPos + ", docno:" + prevDocno);
        seekPoints.add(prevPos);
      }

      System.out.println("offset:" + pos + "\tdocno:" + key + "\tdocid:" + value.getDocid());

      prevPos = pos;
      pos = reader.getPosition();
      prevDocno = key.get();

      cnt++;

      if (cnt > Integer.MAX_VALUE)
        break;
    }

    reader.close();

    reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(path));

    for (long p : seekPoints) {
      reader.seek(p);
      reader.next(key, value);
      System.out.println("seeking to pos " + p + "\tdocno:" + key + "\tdocid:" + value.getDocid());
    }

    reader.close();
  }
}
