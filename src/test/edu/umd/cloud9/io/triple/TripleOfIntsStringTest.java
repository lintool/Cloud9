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

public class TripleOfIntsStringTest {

	@Test
	public void testBasic() throws IOException {
		TripleOfIntsString threeIntsString = new TripleOfIntsString(1, 2, "good");

		assertEquals(threeIntsString.getLeftElement(), 1);
		assertEquals(threeIntsString.getMiddleElement(), 2);
		assertEquals(threeIntsString.getRightElement(), "good");
	}

	@Test
	public void testSerialize() throws IOException {
		TripleOfIntsString origThreeIntsString = new TripleOfIntsString(1, 2, "good");

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origThreeIntsString.write(dataOut);

		TripleOfIntsString threeIntsString = new TripleOfIntsString();

		threeIntsString.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut
				.toByteArray())));

		assertEquals(threeIntsString.getLeftElement(), 1);
		assertEquals(threeIntsString.getMiddleElement(), 2);
		assertEquals(threeIntsString.getRightElement(), "good");
	}

	@Test
	public void testComparison() throws IOException {
		TripleOfIntsString threeInts1 = new TripleOfIntsString(1, 2, "good");
		TripleOfIntsString threeInts2 = new TripleOfIntsString(1, 2, "good");
		TripleOfIntsString threeInts3 = new TripleOfIntsString(1, 2, "buddy");
		TripleOfIntsString threeInts4 = new TripleOfIntsString(1, 1, "good");
		TripleOfIntsString threeInts5 = new TripleOfIntsString(0, 2, "good");

		assertTrue(threeInts1.equals(threeInts2));
		assertFalse(threeInts1.equals(threeInts3));

		assertTrue(threeInts1.compareTo(threeInts2) == 0);
		assertTrue(threeInts1.compareTo(threeInts3) > 0);
		assertTrue(threeInts1.compareTo(threeInts4) > 0);
		assertTrue(threeInts1.compareTo(threeInts5) > 0);
		assertTrue(threeInts2.compareTo(threeInts3) > 0);
		assertTrue(threeInts2.compareTo(threeInts4) > 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(TripleOfIntsStringTest.class);
	}
}
