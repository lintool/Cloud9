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

public class ArrayListOfFloatsWritableTest {
  float neg_one=-1, zero=0, one=1, two=2, three=3, four=4, five=5, six=6, seven=7, nine=9;
  
  @Test
  public void testReadWrite() throws IOException {
    ArrayListOfFloatsWritable arr = new ArrayListOfFloatsWritable();
    arr.add(0, 1.0f).add(1, 3.0f).add(2, 5.0f).add(3, 7.0f);

    SequenceFile.Writer w;
    Configuration conf = new Configuration();
    Path tmp = new Path("tmp");
    FileSystem.get(conf).delete(tmp, true);

    try {
      w = SequenceFile.createWriter(conf, SequenceFile.Writer.file(tmp),
          SequenceFile.Writer.keyClass(IntWritable.class),
          SequenceFile.Writer.valueClass(ArrayListOfFloatsWritable.class));
      w.append(new IntWritable(1), arr);
      w.close();
    } catch (IOException e) {
      e.printStackTrace();
      assertTrue(false);
    }

    List<PairOfWritables<IntWritable, ArrayListOfFloatsWritable>> listOfKeysPairs =
      SequenceFileUtils.<IntWritable, ArrayListOfFloatsWritable> readFile(tmp);
    FileSystem.get(conf).delete(tmp, true);

    assertTrue(listOfKeysPairs.size() == 1);
    ArrayListOfFloatsWritable arrRead = listOfKeysPairs.get(0).getRightElement();
    assertEquals(4, arrRead.size());
    assertEquals(1.0f, arrRead.get(0), 10e-6);
    assertEquals(3.0f, arrRead.get(1), 10e-6);
    assertEquals(5.0f, arrRead.get(2), 10e-6);
    assertEquals(7.0f, arrRead.get(3), 10e-6);

    arrRead.remove(0);
    arrRead.remove(0);
    arrRead.remove(1);

    assertEquals(1, arrRead.size());
    assertEquals(5.0f, arrRead.get(0), 10e-6);
  }

  @Test
  public void testCopyConstructor() {
    ArrayListOfFloatsWritable a = new ArrayListOfFloatsWritable();
    a.add(1.0f).add(3.0f).add(5.0f);

    ArrayListOfFloatsWritable b = new ArrayListOfFloatsWritable(a);
    a.remove(0);
    assertEquals(1.0f, b.get(0), 10e-6);
    assertEquals(3.0f, b.get(1), 10e-6);
    assertEquals(5.0f, b.get(2), 10e-6);
  }

  @Test
  public void testCompare() {
    ArrayListOfFloatsWritable a = new ArrayListOfFloatsWritable();
    a.add(one).add(three).add(five);

    ArrayListOfFloatsWritable b = new ArrayListOfFloatsWritable();

    //[1,3,5] < [1,3,5,7]  
    b.add(one).add(three).add(five).add(seven);
    assertTrue(b.compareTo(a)>0);

    //[1,3,5] = [1,3,5]
    b.remove(3);
    assertTrue(b.compareTo(a)==0);

    //[1,3] < [1,3,5]
    b.remove(2);
    assertTrue(b.compareTo(a)<0);

    //[ ] < [1,3] 
    //[ ] < [1,3,5]
    ArrayListOfFloatsWritable c = new ArrayListOfFloatsWritable();
    assertTrue(b.compareTo(c)>0);
    assertTrue(c.compareTo(a)<0);
    assertTrue(a.compareTo(c)>0);
    assertTrue(c.compareTo(b)<0);


  }

  @Test
  public void testCompare2() {
    // [1, 3, 6]
    ArrayListOfFloatsWritable a = new ArrayListOfFloatsWritable();
    a.add(one).add(three).add(six);

    // [1, 3, 4]
    ArrayListOfFloatsWritable b = new ArrayListOfFloatsWritable();
    b.add(one).add(three).add(four);
    assertTrue(a.compareTo(b)>0);

    // [1, 3, 4, 9]
    ArrayListOfFloatsWritable c = new ArrayListOfFloatsWritable();
    c.add(one).add(three).add(four).add(nine);

    assertTrue(c.compareTo(a)<0);
    assertTrue(b.compareTo(c)<0);

    // [2, 4]
    ArrayListOfFloatsWritable d = new ArrayListOfFloatsWritable();
    d.add(two).add(four);

    // [0, 2]
    ArrayListOfFloatsWritable e = new ArrayListOfFloatsWritable();
    e.add(zero).add(two);

    //[2,4] > all others
    assertTrue(d.compareTo(a)>0);
    assertTrue(d.compareTo(b)>0);
    assertTrue(d.compareTo(c)>0);

    //[0,2] < all others
    assertTrue(e.compareTo(a)<0);
    assertTrue(e.compareTo(b)<0);
    assertTrue(e.compareTo(c)<0);
    assertTrue(e.compareTo(d)<0);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ArrayListOfFloatsWritableTest.class);
  }
}
