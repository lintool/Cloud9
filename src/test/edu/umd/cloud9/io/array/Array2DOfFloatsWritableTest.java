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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class Array2DOfFloatsWritableTest {

  @Test
  public void testBasic() throws IOException {
    float ia[][] = { { 1, 2, 4, 5 }, { 3 }, null };

    Array2DOfFloatsWritable array2D = new Array2DOfFloatsWritable(ia);

    assertEquals(array2D.getValueAt(0, 1), 2.0f, 0.000001);
    assertEquals(array2D.getValueAt(0, 3), 5.0f, 0.000001);
    assertEquals(array2D.getValueAt(1, 0), 3.0f, 0.000001);
    assertEquals(array2D.getValueAt(2, 0), 0.0f, 0.000001);
  }

  @Test
  public void testSerialize1() throws IOException {
    float ia[][] = { { 1, 2, 4, 5 }, { 3 }, null };

    Array2DOfFloatsWritable array2DA = new Array2DOfFloatsWritable(ia);
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    array2DA.write(dataOut);
    Array2DOfFloatsWritable array2DB = new Array2DOfFloatsWritable();
    array2DB.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

    assertEquals(array2DB.getValueAt(0, 1), 2.0f, 0.000001);
    assertEquals(array2DB.getValueAt(0, 3), 5.0f, 0.000001);
    assertEquals(array2DB.getValueAt(1, 0), 3.0f, 0.000001);
    assertEquals(array2DB.getValueAt(2, 0), 0.0f, 0.000001);
  }

  @Test
  public void testToString() {
    float ia[][] = { { 1, 2, 4, 5 }, { 3 }, null };

    Array2DOfFloatsWritable array2D = new Array2DOfFloatsWritable(ia);

    assertEquals(array2D.toString(),
        "[1.0, 2.0, 4.0, 5.0; 3.0, 0.0, 0.0, 0.0; 0.0, 0.0, 0.0, 0.0; ]");
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(Array2DOfFloatsWritableTest.class);
  }
}