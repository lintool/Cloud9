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

package edu.umd.cloud9.tuple;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class PairOfStringsWritableComparableTest {

	@Test
	public void testBasic() throws IOException {
		PairOfStringsWritableComparable pair = new PairOfStringsWritableComparable();

		pair.set("hi", "there");

		assertEquals(pair.getLeftElement(), "hi");
		assertEquals(pair.getRightElement(), "there");
	}

	@Test
	public void testSerialize() throws IOException {
		PairOfStringsWritableComparable origPair = new PairOfStringsWritableComparable();

		origPair.set("hi", "there");

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origPair.write(dataOut);

		PairOfStringsWritableComparable pair = new PairOfStringsWritableComparable();

		pair.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut
				.toByteArray())));

		assertEquals(pair.getLeftElement(), "hi");
		assertEquals(pair.getRightElement(), "there");
	}

	@Test
	public void testComparison() throws IOException {
		PairOfStringsWritableComparable pair1 = new PairOfStringsWritableComparable();
		pair1.set("hi", "there");

		PairOfStringsWritableComparable pair2 = new PairOfStringsWritableComparable();
		pair2.set("hi", "there");

		PairOfStringsWritableComparable pair3 = new PairOfStringsWritableComparable();
		pair3.set("hi", "howdy");

		PairOfStringsWritableComparable pair4 = new PairOfStringsWritableComparable();
		pair4.set("a", "howdy");

		PairOfStringsWritableComparable pair5 = new PairOfStringsWritableComparable();
		pair5.set("hi", "z");
		
		assertTrue(pair1.equals(pair2));
		assertFalse(pair1.equals(pair3));

		assertTrue(pair1.compareTo(pair2) == 0);
		assertTrue(pair1.compareTo(pair3) > 0);
		assertTrue(pair1.compareTo(pair4) > 0);
		assertTrue(pair1.compareTo(pair5) < 0);
		assertTrue(pair3.compareTo(pair4) > 0);
		assertTrue(pair4.compareTo(pair5) < 0);		
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PairOfStringsWritableComparableTest.class);
	}

}
