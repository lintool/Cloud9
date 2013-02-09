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
import edu.umd.cloud9.io.pair.PairOfStrings;

public class PairOfStringsTest {

	@Test
	public void testBasic() throws IOException {
		PairOfStrings pair = new PairOfStrings("hi", "there");

		assertEquals("hi", pair.getLeftElement());
		assertEquals("there", pair.getRightElement());
	}

	@Test
	public void testSerialize() throws IOException {
		PairOfStrings origPair = new PairOfStrings("hi", "there");

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origPair.write(dataOut);

		PairOfStrings pair = new PairOfStrings();

		pair.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals("hi", pair.getLeftElement());
		assertEquals("there", pair.getRightElement());
	}

	@Test
	public void testOptimizedSerialize() throws IOException {
		PairOfStrings pair1 = new PairOfStrings("hi", "there");
		ByteArrayOutputStream pair1_bytesOut = new ByteArrayOutputStream();
		DataOutputStream pair1_dataOut = new DataOutputStream(pair1_bytesOut);
		pair1.write(pair1_dataOut);
		byte[] bytes1 = pair1_bytesOut.toByteArray();

		PairOfStrings pair2 = new PairOfStrings("hi", "there");
		ByteArrayOutputStream pair2_bytesOut = new ByteArrayOutputStream();
		DataOutputStream pair2_dataOut = new DataOutputStream(pair2_bytesOut);
		pair2.write(pair2_dataOut);
		byte[] bytes2 = pair2_bytesOut.toByteArray();

		PairOfStrings pair3 = new PairOfStrings("hi", "howdy");
		ByteArrayOutputStream pair3_bytesOut = new ByteArrayOutputStream();
		DataOutputStream pair3_dataOut = new DataOutputStream(pair3_bytesOut);
		pair3.write(pair3_dataOut);
		byte[] bytes3 = pair3_bytesOut.toByteArray();

		PairOfStrings pair4 = new PairOfStrings("a", "howdy");
		ByteArrayOutputStream pair4_bytesOut = new ByteArrayOutputStream();
		DataOutputStream pair4_dataOut = new DataOutputStream(pair4_bytesOut);
		pair4.write(pair4_dataOut);
		byte[] bytes4 = pair4_bytesOut.toByteArray();

		PairOfStrings pair5 = new PairOfStrings("hi", "z");
		ByteArrayOutputStream pair5_bytesOut = new ByteArrayOutputStream();
		DataOutputStream pair5_dataOut = new DataOutputStream(pair5_bytesOut);
		pair5.write(pair5_dataOut);
		byte[] bytes5 = pair5_bytesOut.toByteArray();

    PairOfStrings.Comparator pairOfStringComparator = new PairOfStrings.Comparator();
    assertTrue(pairOfStringComparator.compare(bytes1, 0, bytes1.length, bytes2, 0, bytes2.length) == 0);
    assertFalse(pair1.equals(pair3));

    assertTrue(pairOfStringComparator.compare(bytes1, 0, bytes1.length, bytes3, 0, bytes3.length) > 0);
    assertTrue(pairOfStringComparator.compare(bytes1, 0, bytes1.length, bytes4, 0, bytes4.length) > 0);
    assertTrue(pairOfStringComparator.compare(bytes1, 0, bytes1.length, bytes5, 0, bytes5.length) < 0);
    assertTrue(pairOfStringComparator.compare(bytes3, 0, bytes3.length, bytes4, 0, bytes4.length) > 0);
    assertTrue(pairOfStringComparator.compare(bytes4, 0, bytes4.length, bytes5, 0, bytes5.length) < 0);
	}

	@Test
	public void testComparison1() throws IOException {
		PairOfStrings pair1 = new PairOfStrings("hi", "there");
		PairOfStrings pair2 = new PairOfStrings("hi", "there");
		PairOfStrings pair3 = new PairOfStrings("hi", "howdy");
		PairOfStrings pair4 = new PairOfStrings("a", "howdy");
		PairOfStrings pair5 = new PairOfStrings("hi", "z");
		
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
		WritableComparator comparator = new PairOfStrings.Comparator();

		PairOfStrings pair1 = new PairOfStrings("hi", "there");
		PairOfStrings pair2 = new PairOfStrings("hi", "there");
		PairOfStrings pair3 = new PairOfStrings("hi", "howdy");
		PairOfStrings pair4 = new PairOfStrings("a", "howdy");
		PairOfStrings pair5 = new PairOfStrings("hi", "z");
		
		assertTrue(pair1.equals(pair2));
		assertFalse(pair1.equals(pair3));

		assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair2) == 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair3) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair4) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair5) < 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, pair3, pair4) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, pair4, pair5) < 0);		
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PairOfStringsTest.class);
	}
}
