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

public class ArrayListOfIntsWritableTest {

  @Test
  public void testToString() {
    assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]", new ArrayListOfIntsWritable(1, 11).toString());
    assertEquals("[1, 2, 3, 4, 5 ... (5 more) ]", new ArrayListOfIntsWritable(1, 11).toString(5));

    assertEquals("[1, 2, 3, 4, 5]", new ArrayListOfIntsWritable(1, 6).toString());
    assertEquals("[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]", new ArrayListOfIntsWritable(1, 12).toString());

    assertEquals("[]", new ArrayListOfIntsWritable().toString());
  }

  @Test
  public void testReadWrite() throws IOException {
    ArrayListOfIntsWritable arr = new ArrayListOfIntsWritable();
    arr.add(0, 1).add(1, 3).add(2, 5).add(3, 7);

    FileSystem fs;
    SequenceFile.Writer w;
    Configuration conf = new Configuration();
    Path tmp = new Path("tmp");

    try {
      fs = FileSystem.get(conf);
      w = SequenceFile.createWriter(fs, conf, tmp, IntWritable.class, ArrayListOfIntsWritable.class);
      w.append(new IntWritable(1), arr);
      w.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    List<PairOfWritables<IntWritable, ArrayListOfIntsWritable>> listOfKeysPairs =
      SequenceFileUtils.<IntWritable, ArrayListOfIntsWritable> readFile(tmp);
    FileSystem.get(conf).delete(tmp, true);

    assertTrue(listOfKeysPairs.size() == 1);
    ArrayListOfIntsWritable arrRead = listOfKeysPairs.get(0).getRightElement();
    assertEquals(4, arrRead.size());
    assertEquals(1, arrRead.get(0));
    assertEquals(3, arrRead.get(1));
    assertEquals(5, arrRead.get(2));
    assertEquals(7, arrRead.get(3));

    arrRead.remove(0);
    arrRead.remove(0);
    arrRead.remove(1);

    assertEquals(1, arrRead.size());
    assertEquals(5, arrRead.get(0));
  }

  @Test
  public void testCopyConstructor() {
    ArrayListOfIntsWritable a = new ArrayListOfIntsWritable();
    a.add(1).add(3).add(5);

    ArrayListOfIntsWritable b = new ArrayListOfIntsWritable(a);
    a.remove(0);
    assertEquals(1, b.get(0));
    assertEquals(3, b.get(1));
    assertEquals(5, b.get(2));
  }

  @Test
  public void testCompare() {
    ArrayListOfIntsWritable a = new ArrayListOfIntsWritable();
    a.add(1).add(3).add(5);

    ArrayListOfIntsWritable b = new ArrayListOfIntsWritable();

    //[1,3,5] < [1,3,5,7]  
    b.add(1).add(3).add(5).add(7);
    assertTrue(b.compareTo(a)>0);

    //[1,3,5] = [1,3,5]
    b.remove(7);
    assertTrue(b.compareTo(a)==0);

    //[1,3] < [1,3,5]
    b.remove(5);
    assertTrue(b.compareTo(a)<0);

    //[ ] < [1,3] 
    //[ ] < [1,3,5]
    ArrayListOfIntsWritable c = new ArrayListOfIntsWritable();
    assertTrue(b.compareTo(c)>0);
    assertTrue(c.compareTo(a)<0);
    assertTrue(a.compareTo(c)>0);
    assertTrue(c.compareTo(b)<0);


  }

  @Test
  public void testCompare2() {
    // [1, 3, 6]
    ArrayListOfIntsWritable a = new ArrayListOfIntsWritable();
    a.add(1).add(3).add(6);

    // [1, 3, 4]
    ArrayListOfIntsWritable b = new ArrayListOfIntsWritable();
    b.add(1).add(3).add(4);
    assertTrue(a.compareTo(b)>0);

    // [1, 3, 4, 9]
    ArrayListOfIntsWritable c = new ArrayListOfIntsWritable();
    c.add(1).add(3).add(4).add(9);

    assertTrue(c.compareTo(a)<0);
    assertTrue(b.compareTo(c)<0);

    // [2, 4]
    ArrayListOfIntsWritable d = new ArrayListOfIntsWritable();
    d.add(2).add(4);

    // [0, 2]
    ArrayListOfIntsWritable e = new ArrayListOfIntsWritable();
    e.add(0).add(2);

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

  @Test
  public void testIO(){
    ArrayListOfIntsWritable a1 = new ArrayListOfIntsWritable();
    ArrayListOfIntsWritable a2 = new ArrayListOfIntsWritable();
    ArrayListOfIntsWritable a3 = new ArrayListOfIntsWritable();

    a1.add(1);
    a1.add(2);

    a2.add(3);

    FileSystem fs;
    SequenceFile.Writer w;
    Configuration conf = new Configuration();

    try {
      fs = FileSystem.get(conf);
      w = SequenceFile.createWriter(fs, conf, new Path("test"),
          ArrayListOfIntsWritable.class, IntWritable.class);  
      System.out.println(a1.toString());
      System.out.println(a1.size());
      System.out.println(a3);
      System.out.println(a3.size());
      w.append(a1, new IntWritable(1));
      w.append(a2, new IntWritable(2));
      w.append(a3, new IntWritable(3));
      w.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    try {
      SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), new Path("test"), conf);

      ArrayListOfIntsWritable key = (ArrayListOfIntsWritable) reader.getKeyClass().newInstance();
      IntWritable value = (IntWritable) reader.getValueClass().newInstance();
      while (reader.next(key, value)) {
        int docno = value.get();
        System.out.println(docno);
        System.out.println(key.toString());
        System.out.println(key.size());
      }
      reader.close();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }

  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ArrayListOfIntsWritableTest.class);
  }
}
