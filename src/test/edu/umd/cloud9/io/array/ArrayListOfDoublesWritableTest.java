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

package edu.umd.cloud9.io.array;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;

import edu.umd.cloud9.io.SequenceFileUtils;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class ArrayListOfDoublesWritableTest {

  @Test
  public void testReadWrite() throws IOException {
    ArrayListOfDoublesWritable arr = new ArrayListOfDoublesWritable();
    arr.add(0, 1.0).add(1, 3.0).add(2, 5.0).add(3, 7.0);

    FileSystem fs;
    SequenceFile.Writer w;
    Configuration conf = new Configuration();
    Path tmp = new Path("tmp");

    try {
      fs = FileSystem.get(conf);
      w = SequenceFile.createWriter(fs, conf, tmp, IntWritable.class, ArrayListOfDoublesWritable.class);
      w.append(new IntWritable(1), arr);
      w.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    List<PairOfWritables<IntWritable, ArrayListOfDoublesWritable>> listOfKeysPairs =
      SequenceFileUtils.<IntWritable, ArrayListOfDoublesWritable> readFile(tmp);
    FileSystem.get(conf).delete(tmp, true);

    assertTrue(listOfKeysPairs.size() == 1);
    ArrayListOfDoublesWritable arrRead = listOfKeysPairs.get(0).getRightElement();
    assertEquals(4, arrRead.size());
    assertEquals(1.0, arrRead.get(0), 10e-6);
    assertEquals(3.0, arrRead.get(1), 10e-6);
    assertEquals(5.0, arrRead.get(2), 10e-6);
    assertEquals(7.0, arrRead.get(3), 10e-6);

    arrRead.remove(0);
    arrRead.remove(0);
    arrRead.remove(1);

    assertEquals(1, arrRead.size());
    assertEquals(5.0, arrRead.get(0), 10e-6);
  }

  @Test
  public void testCopyConstructor() {
    ArrayListOfDoublesWritable a = new ArrayListOfDoublesWritable();
    a.add(1.0).add(3.0).add(5.0);

    ArrayListOfDoublesWritable b = new ArrayListOfDoublesWritable(a);
    a.remove(0);
    assertEquals(1.0, b.get(0), 10e-6);
    assertEquals(3.0, b.get(1), 10e-6);
    assertEquals(5.0, b.get(2), 10e-6);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ArrayListOfDoublesWritableTest.class);
  }
}
