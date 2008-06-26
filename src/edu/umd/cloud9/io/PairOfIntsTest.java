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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

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

		pair.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut
				.toByteArray())));

		assertEquals(pair.getLeftElement(), 1);
		assertEquals(pair.getRightElement(), 2);
	}

	@Test
	public void testComparison() throws IOException {
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

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PairOfIntsTest.class);
	}

}
