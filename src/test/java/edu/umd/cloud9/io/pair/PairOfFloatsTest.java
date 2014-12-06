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
import edu.umd.cloud9.io.pair.PairOfFloats;

public class PairOfFloatsTest {

	@Test
	public void testBasic() throws IOException {
		PairOfFloats pair = new PairOfFloats(3.14f, 2.0f);

		assertTrue(pair.getLeftElement() == 3.14f);
		assertTrue(pair.getRightElement() == 2.0f);
	}

	@Test
	public void testSerialize() throws IOException {
		PairOfFloats origPair = new PairOfFloats(3.14f, 2.0f);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origPair.write(dataOut);

		PairOfFloats pair = new PairOfFloats();

		pair.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertTrue(pair.getLeftElement() == 3.14f);
		assertTrue(pair.getRightElement() == 2.0f);
	}

	@Test
	public void testComparison1() throws IOException {
		PairOfFloats pair1 = new PairOfFloats(3.14f, 2.0f);
		PairOfFloats pair2 = new PairOfFloats(3.14f, 2.0f);
		PairOfFloats pair3 = new PairOfFloats(3.14f, 1.0f);
		PairOfFloats pair4 = new PairOfFloats(0.3f, 9.0f);
		PairOfFloats pair5 = new PairOfFloats(9.9f, 0.0f);

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
		WritableComparator comparator = new PairOfFloats.Comparator();

		PairOfFloats pair1 = new PairOfFloats(3.14f, 2.0f);
		PairOfFloats pair2 = new PairOfFloats(3.14f, 2.0f);
		PairOfFloats pair3 = new PairOfFloats(3.14f, 1.0f);
		PairOfFloats pair4 = new PairOfFloats(0.3f, 9.0f);
		PairOfFloats pair5 = new PairOfFloats(9.9f, 0.0f);

		assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair2) == 0);
		assertFalse(WritableComparatorTestHarness.compare(comparator, pair1, pair3) == 0);

		assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair2) == 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair3) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair4) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, pair1, pair5) < 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, pair3, pair4) > 0);
		assertTrue(WritableComparatorTestHarness.compare(comparator, pair4, pair5) < 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PairOfFloatsTest.class);
	}

}
