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

package edu.umd.cloud9.io.pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.WritableComparator;
import org.junit.Test;

import edu.umd.cloud9.debug.WritableComparatorTestHarness;
import edu.umd.cloud9.io.pair.PairOfIntFloat;

public class PairOfIntFloatTest {

  @Test
  public void testBasic() throws IOException {
    PairOfIntFloat pair = new PairOfIntFloat(1, 2.0f);

    assertEquals(1.0f, pair.getLeftElement(), 10e-6);
    assertEquals(2.0f, pair.getRightElement(), 10e-6);
  }

  @Test
  public void testSerialize() throws IOException {
    PairOfIntFloat origPair = new PairOfIntFloat(1, 2.0f);

    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);

    origPair.write(dataOut);

    PairOfIntFloat pair = new PairOfIntFloat();

    pair.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

    assertEquals(1.0f, pair.getLeftElement(), 10e-6);
    assertEquals(2.0f, pair.getRightElement(), 10e-6);
  }

  @Test
  public void testComparison1() throws IOException {
    PairOfIntFloat pair1 = new PairOfIntFloat(1, 2.0f);
    PairOfIntFloat pair2 = new PairOfIntFloat(1, 2.0f);
    PairOfIntFloat pair3 = new PairOfIntFloat(1, 1.0f);
    PairOfIntFloat pair4 = new PairOfIntFloat(0, 9.0f);
    PairOfIntFloat pair5 = new PairOfIntFloat(9, 0.0f);

    assertTrue(pair1.equals(pair2));
    assertFalse(pair1.equals(pair3));

    assertTrue(pair1.compareTo(pair2) == 0);
    assertTrue(pair1.compareTo(pair3) > 0);
    assertTrue(pair1.compareTo(pair4) > 0);
    assertTrue(pair1.compareTo(pair5) < 0);
    assertTrue(pair3.compareTo(pair4) > 0);
    assertTrue(pair4.compareTo(pair5) < 0);
  }

  @Test
  public void testComparison2() throws IOException {
    WritableComparator comparator = new PairOfIntFloat.Comparator();

    PairOfIntFloat pair1 = new PairOfIntFloat(1, 2.0f);
    PairOfIntFloat pair2 = new PairOfIntFloat(1, 2.0f);
    PairOfIntFloat pair3 = new PairOfIntFloat(1, 1.0f);
    PairOfIntFloat pair4 = new PairOfIntFloat(0, 9.0f);
    PairOfIntFloat pair5 = new PairOfIntFloat(9, 0.0f);

    assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair2) == 0);
    assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair3) > 0);
    assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair4) > 0);
    assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair5) < 0);
    assertTrue(WritableComparatorTestHarness.compare(comparator, pair3, pair4) > 0);
    assertTrue(WritableComparatorTestHarness.compare(comparator, pair4, pair5) < 0);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(PairOfIntFloatTest.class);
  }
}
