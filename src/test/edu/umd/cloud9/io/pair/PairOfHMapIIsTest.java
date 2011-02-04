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

package edu.umd.cloud9.io.pair;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.io.map.HMapIIW;
import edu.umd.cloud9.util.map.HMapII;

public class PairOfHMapIIsTest {

	@Test
	public void testBasic() throws IOException {
		HMapII m1 = new HMapII();
		HMapII m2 = new HMapII();

		m1.put(2, 5);
		m1.put(1, 22);

		m2.put(1, 2);
		m2.put(2, 4);
		m2.put(3, 6);

		PairOfHMapIIs m = new PairOfHMapIIs(m1, m2);

		assertEquals(m.getLeftElement().size(), 2);
		assertEquals(m.getRightElement().size(), 3);
	}

	@Test
	public void testSerialize() throws IOException {
		HMapII m1 = new HMapII();
		HMapII m2 = new HMapII();

		m1.put(2, 5);
		m1.put(1, 22);

		m2.put(1, 2);
		m2.put(2, 4);
		m2.put(3, 6);

		PairOfHMapIIs m = new PairOfHMapIIs(m1, m2);

		assertEquals(m.getLeftElement().size(), 2);
		assertEquals(m.getLeftElement().get(2), 5);
		assertEquals(m.getLeftElement().get(1), 22);

		assertEquals(m.getRightElement().size(), 3);
		assertEquals(m.getRightElement().get(1), 2);
		assertEquals(m.getRightElement().get(2), 4);
		assertEquals(m.getRightElement().get(3), 6);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m.write(dataOut);

		PairOfHMapIIs n1 = new PairOfHMapIIs();

		n1.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(n1.getLeftElement().size(), 2);
		assertEquals(n1.getLeftElement().get(2), 5);
		assertEquals(n1.getLeftElement().get(1), 22);

		assertEquals(n1.getRightElement().size(), 3);
		assertEquals(n1.getRightElement().get(1), 2);
		assertEquals(n1.getRightElement().get(2), 4);
		assertEquals(n1.getRightElement().get(3), 6);

		PairOfHMapIIs n2 = PairOfHMapIIs.create(m.serialize());

		assertEquals(n2.getLeftElement().size(), 2);
		assertEquals(n2.getLeftElement().get(2), 5);
		assertEquals(n2.getLeftElement().get(1), 22);

		assertEquals(n2.getRightElement().size(), 3);
		assertEquals(n2.getRightElement().get(1), 2);
		assertEquals(n2.getRightElement().get(2), 4);
		assertEquals(n2.getRightElement().get(3), 6);
	}

	@Test
	public void testSerializeEmptyRight() throws IOException {
		HMapII m1 = new HMapII();
		HMapII m2 = new HMapII();

		m1.put(2, 5);
		m1.put(1, 22);

		PairOfHMapIIs m = new PairOfHMapIIs(m1, m2);

		assertEquals(m.getLeftElement().size(), 2);
		assertEquals(m.getLeftElement().get(2), 5);
		assertEquals(m.getLeftElement().get(1), 22);

		assertEquals(m.getRightElement().size(), 0);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m.write(dataOut);

		PairOfHMapIIs n1 = new PairOfHMapIIs();

		n1.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(n1.getLeftElement().size(), 2);
		assertEquals(n1.getLeftElement().get(2), 5);
		assertEquals(n1.getLeftElement().get(1), 22);

		assertEquals(m.getRightElement().size(), 0);

		PairOfHMapIIs n2 = PairOfHMapIIs.create(m.serialize());

		assertEquals(n2.getLeftElement().size(), 2);
		assertEquals(n2.getLeftElement().get(2), 5);
		assertEquals(n2.getLeftElement().get(1), 22);

		assertEquals(m.getRightElement().size(), 0);
	}

	@Test
	public void testSerializeEmptyLeft() throws IOException {
		HMapII m1 = new HMapII();
		HMapII m2 = new HMapII();

		m2.put(1, 2);
		m2.put(2, 4);
		m2.put(3, 6);

		PairOfHMapIIs m = new PairOfHMapIIs(m1, m2);

		assertEquals(m.getLeftElement().size(), 0);

		assertEquals(m.getRightElement().size(), 3);
		assertEquals(m.getRightElement().get(1), 2);
		assertEquals(m.getRightElement().get(2), 4);
		assertEquals(m.getRightElement().get(3), 6);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m.write(dataOut);

		PairOfHMapIIs n1 = new PairOfHMapIIs();

		n1.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(n1.getLeftElement().size(), 0);

		assertEquals(n1.getRightElement().size(), 3);
		assertEquals(n1.getRightElement().get(1), 2);
		assertEquals(n1.getRightElement().get(2), 4);
		assertEquals(n1.getRightElement().get(3), 6);

		PairOfHMapIIs n2 = PairOfHMapIIs.create(m.serialize());

		assertEquals(n2.getLeftElement().size(), 0);

		assertEquals(n2.getRightElement().size(), 3);
		assertEquals(n2.getRightElement().get(1), 2);
		assertEquals(n2.getRightElement().get(2), 4);
		assertEquals(n2.getRightElement().get(3), 6);
	}

	@Test
	public void testSerializeEmptyBoth() throws IOException {
		HMapII m1 = new HMapII();
		HMapII m2 = new HMapII();

		PairOfHMapIIs m = new PairOfHMapIIs(m1, m2);

		assertEquals(m.getLeftElement().size(), 0);
		assertEquals(m.getRightElement().size(), 0);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m.write(dataOut);

		PairOfHMapIIs n1 = new PairOfHMapIIs();

		n1.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(n1.getLeftElement().size(), 0);
		assertEquals(n1.getRightElement().size(), 0);

		PairOfHMapIIs n2 = PairOfHMapIIs.create(m.serialize());

		assertEquals(n2.getLeftElement().size(), 0);
		assertEquals(n2.getRightElement().size(), 0);
	}

	@Test
	public void testSerializeLazy() throws IOException {
		HMapIIW.setLazyDecodeFlag(true);

		HMapII m1 = new HMapII();
		HMapII m2 = new HMapII();

		m1.put(2, 5);
		m1.put(1, 22);

		m2.put(1, 2);
		m2.put(2, 4);
		m2.put(3, 6);

		PairOfHMapIIs m = new PairOfHMapIIs(m1, m2);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m.write(dataOut);

		PairOfHMapIIs n1 = new PairOfHMapIIs();

		n1.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(n1.getLeftElement().size(), 2);
		assertEquals(n1.getLeftElement().get(2), 5);
		assertEquals(n1.getLeftElement().get(1), 22);

		assertEquals(n1.getRightElement().size(), 3);
		assertEquals(n1.getRightElement().get(1), 2);
		assertEquals(n1.getRightElement().get(2), 4);
		assertEquals(n1.getRightElement().get(3), 6);

		PairOfHMapIIs n2 = PairOfHMapIIs.create(m.serialize());

		assertEquals(n2.getLeftElement().size(), 2);
		assertEquals(n2.getLeftElement().get(2), 5);
		assertEquals(n2.getLeftElement().get(1), 22);

		assertEquals(n2.getRightElement().size(), 3);
		assertEquals(n2.getRightElement().get(1), 2);
		assertEquals(n2.getRightElement().get(2), 4);
		assertEquals(n2.getRightElement().get(3), 6);
	}

	@Test
	public void testSerializeLazyEmptyLeft() throws IOException {
		HMapIIW.setLazyDecodeFlag(true);

		HMapII m1 = new HMapII();
		HMapII m2 = new HMapII();

		m2.put(1, 2);
		m2.put(2, 4);
		m2.put(3, 6);

		PairOfHMapIIs m = new PairOfHMapIIs(m1, m2);

		assertEquals(m.getLeftElement().size(), 0);

		assertEquals(m.getRightElement().size(), 3);
		assertEquals(m.getRightElement().get(1), 2);
		assertEquals(m.getRightElement().get(2), 4);
		assertEquals(m.getRightElement().get(3), 6);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m.write(dataOut);

		PairOfHMapIIs n1 = new PairOfHMapIIs();

		n1.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(n1.getLeftElement().size(), 0);

		assertEquals(n1.getRightElement().size(), 3);
		assertEquals(n1.getRightElement().get(1), 2);
		assertEquals(n1.getRightElement().get(2), 4);
		assertEquals(n1.getRightElement().get(3), 6);

		PairOfHMapIIs n2 = PairOfHMapIIs.create(m.serialize());

		assertEquals(n2.getLeftElement().size(), 0);

		assertEquals(n2.getRightElement().size(), 3);
		assertEquals(n2.getRightElement().get(1), 2);
		assertEquals(n2.getRightElement().get(2), 4);
		assertEquals(n2.getRightElement().get(3), 6);
	}

	@Test
	public void testSerializeLazyEmptyRight() throws IOException {
		HMapIIW.setLazyDecodeFlag(true);

		HMapII m1 = new HMapII();
		HMapII m2 = new HMapII();

		m1.put(2, 5);
		m1.put(1, 22);

		PairOfHMapIIs m = new PairOfHMapIIs(m1, m2);

		assertEquals(m.getLeftElement().size(), 2);
		assertEquals(m.getLeftElement().get(2), 5);
		assertEquals(m.getLeftElement().get(1), 22);

		assertEquals(m.getRightElement().size(), 0);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m.write(dataOut);

		PairOfHMapIIs n1 = new PairOfHMapIIs();

		n1.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(n1.getLeftElement().size(), 2);
		assertEquals(n1.getLeftElement().get(2), 5);
		assertEquals(n1.getLeftElement().get(1), 22);

		assertEquals(n1.getRightElement().size(), 0);

		PairOfHMapIIs n2 = PairOfHMapIIs.create(m.serialize());

		assertEquals(n2.getLeftElement().size(), 2);
		assertEquals(n2.getLeftElement().get(2), 5);
		assertEquals(n2.getLeftElement().get(1), 22);

		assertEquals(n2.getRightElement().size(), 0);
	}

	@Test
	public void testSerializeLazyEmptyBoth() throws IOException {
		HMapIIW.setLazyDecodeFlag(true);

		HMapII m1 = new HMapII();
		HMapII m2 = new HMapII();

		PairOfHMapIIs m = new PairOfHMapIIs(m1, m2);

		assertEquals(m.getLeftElement().size(), 0);
		assertEquals(m.getRightElement().size(), 0);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m.write(dataOut);

		PairOfHMapIIs n1 = new PairOfHMapIIs();

		n1.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(n1.getLeftElement().size(), 0);
		assertEquals(n1.getRightElement().size(), 0);

		PairOfHMapIIs n2 = PairOfHMapIIs.create(m.serialize());

		assertEquals(n2.getLeftElement().size(), 0);
		assertEquals(n2.getRightElement().size(), 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PairOfHMapIIsTest.class);
	}
}
