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

package edu.umd.cloud9.io.triple;

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

public class TripleOfIntsTest {

	@Test
	public void testBasic() throws IOException {
		TripleOfInts threeInts = new TripleOfInts(1, 2, 3);

		assertEquals(threeInts.getLeftElement(), 1);
		assertEquals(threeInts.getMiddleElement(), 2);
		assertEquals(threeInts.getRightElement(), 3);
	}

	@Test
	public void testSerialize() throws IOException {
		TripleOfInts origThreeInts = new TripleOfInts(1, 2, 3);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origThreeInts.write(dataOut);

		TripleOfInts threeInts = new TripleOfInts();

		threeInts.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(threeInts.getLeftElement(), 1);
		assertEquals(threeInts.getMiddleElement(), 2);
		assertEquals(threeInts.getRightElement(), 3);
	}

	@Test
	public void testComparison1() throws IOException {
		TripleOfInts threeInts1 = new TripleOfInts(1, 2, 3);
		TripleOfInts threeInts2 = new TripleOfInts(1, 2, 3);
		TripleOfInts threeInts3 = new TripleOfInts(1, 2, 2);
		TripleOfInts threeInts4 = new TripleOfInts(1, 1, 3);
		TripleOfInts threeInts5 = new TripleOfInts(0, 2, 3);

		assertTrue(threeInts1.equals(threeInts2));
		assertFalse(threeInts1.equals(threeInts3));

		assertTrue(threeInts1.compareTo(threeInts2) == 0);
		assertTrue(threeInts1.compareTo(threeInts3) > 0);
		assertTrue(threeInts1.compareTo(threeInts4) > 0);
		assertTrue(threeInts1.compareTo(threeInts5) > 0);
		assertTrue(threeInts2.compareTo(threeInts3) > 0);
		assertTrue(threeInts2.compareTo(threeInts4) > 0);
	}

	@Test
	public void testComparison2() throws IOException {
		WritableComparator comparator = new TripleOfInts.Comparator();

		TripleOfInts threeInts1 = new TripleOfInts(1, 2, 3);
		TripleOfInts threeInts2 = new TripleOfInts(1, 2, 3);
		TripleOfInts threeInts3 = new TripleOfInts(1, 2, 2);
		TripleOfInts threeInts4 = new TripleOfInts(1, 1, 3);
		TripleOfInts threeInts5 = new TripleOfInts(0, 2, 3);

		assertTrue(WritableComparatorTestHarness.compare(comparator, threeInts1, threeInts2) == 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, threeInts1, threeInts3) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, threeInts1, threeInts4) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, threeInts1, threeInts5) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, threeInts2, threeInts3) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, threeInts2, threeInts4) > 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(TripleOfIntsTest.class);
	}
}
