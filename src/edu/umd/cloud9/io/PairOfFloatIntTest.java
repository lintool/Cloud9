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

import org.junit.Test;

public class PairOfFloatIntTest {

	@Test
	public void testBasic() throws IOException {
		PairOfFloatInt pair = new PairOfFloatInt();

		pair.set(3.14f, 2);

		assertTrue(pair.getLeftElement() == 3.14f);
		assertEquals(2, pair.getRightElement());
	}

	@Test
	public void testSerialize() throws IOException {
		PairOfFloatInt origPair = new PairOfFloatInt();

		origPair.set(3.14f, 2);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		origPair.write(dataOut);

		PairOfFloatInt pair = new PairOfFloatInt();

		pair.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertTrue(pair.getLeftElement() == 3.14f);
		assertEquals(pair.getRightElement(), 2);
	}

	@Test
	public void testComparison1() throws IOException {
		PairOfFloatInt pair1 = new PairOfFloatInt();
		pair1.set(3.14f, 2);

		PairOfFloatInt pair2 = new PairOfFloatInt();
		pair2.set(3.14f, 2);

		PairOfFloatInt pair3 = new PairOfFloatInt();
		pair3.set(3.14f, 1);

		PairOfFloatInt pair4 = new PairOfFloatInt();
		pair4.set(0.3f, 9);

		PairOfFloatInt pair5 = new PairOfFloatInt();
		pair5.set(9.9f, 0);

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
		return new JUnit4TestAdapter(PairOfFloatIntTest.class);
	}

}
