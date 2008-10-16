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

package edu.umd.cloud9.io;

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

public class PairOfIntsTest {

	@Test
	public void testBasic() throws IOException {
		PairOfInts pair = new PairOfInts();

		pair.set(1, 2);

		assertEquals(pair.getLeftElement(), 1);
		assertEquals(pair.getRightElement(), 2);
	}

	@Test
	public void testSerialize() throws IOException {
		PairOfInts origPair = new PairOfInts();

		origPair.set(1, 2);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origPair.write(dataOut);

		PairOfInts pair = new PairOfInts();

		pair.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(pair.getLeftElement(), 1);
		assertEquals(pair.getRightElement(), 2);
	}

	@Test
	public void testComparison1() throws IOException {
		PairOfInts pair1 = new PairOfInts();
		pair1.set(1, 2);

		PairOfInts pair2 = new PairOfInts();
		pair2.set(1, 2);

		PairOfInts pair3 = new PairOfInts();
		pair3.set(1, 1);

		PairOfInts pair4 = new PairOfInts();
		pair4.set(0, 9);

		PairOfInts pair5 = new PairOfInts();
		pair5.set(9, 0);

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
		WritableComparator comparator = new PairOfInts.Comparator();

		PairOfInts pair1 = new PairOfInts();
		pair1.set(1, 2);

		ByteArrayOutputStream bytesOut1 = new ByteArrayOutputStream();
		DataOutputStream dataOut1 = new DataOutputStream(bytesOut1);
		pair1.write(dataOut1);
		byte[] bytes1 = bytesOut1.toByteArray();

		PairOfInts pair2 = new PairOfInts();
		pair2.set(1, 2);

		ByteArrayOutputStream bytesOut2 = new ByteArrayOutputStream();
		DataOutputStream dataOut2 = new DataOutputStream(bytesOut2);
		pair2.write(dataOut2);
		byte[] bytes2 = bytesOut2.toByteArray();

		PairOfInts pair3 = new PairOfInts();
		pair3.set(1, 1);

		ByteArrayOutputStream bytesOut3 = new ByteArrayOutputStream();
		DataOutputStream dataOut3 = new DataOutputStream(bytesOut3);
		pair3.write(dataOut3);
		byte[] bytes3 = bytesOut3.toByteArray();

		PairOfInts pair4 = new PairOfInts();
		pair4.set(0, 9);

		ByteArrayOutputStream bytesOut4 = new ByteArrayOutputStream();
		DataOutputStream dataOut4 = new DataOutputStream(bytesOut4);
		pair4.write(dataOut4);
		byte[] bytes4 = bytesOut4.toByteArray();

		PairOfInts pair5 = new PairOfInts();
		pair5.set(9, 0);

		ByteArrayOutputStream bytesOut5 = new ByteArrayOutputStream();
		DataOutputStream dataOut5 = new DataOutputStream(bytesOut5);
		pair5.write(dataOut5);
		byte[] bytes5 = bytesOut5.toByteArray();

		assertTrue(pair1.equals(pair2));
		assertFalse(pair1.equals(pair3));

		assertTrue(comparator.compare(bytes1, 0, bytes1.length, bytes2, 0, bytes2.length) == 0);
		assertTrue(comparator.compare(bytes1, 0, bytes1.length, bytes3, 0, bytes3.length) > 0);
		assertTrue(comparator.compare(bytes1, 0, bytes1.length, bytes4, 0, bytes4.length) > 0);
		assertTrue(comparator.compare(bytes1, 0, bytes1.length, bytes5, 0, bytes5.length) < 0);
		assertTrue(comparator.compare(bytes3, 0, bytes3.length, bytes4, 0, bytes4.length) > 0);
		assertTrue(comparator.compare(bytes4, 0, bytes4.length, bytes5, 0, bytes5.length) < 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PairOfIntsTest.class);
	}

}
