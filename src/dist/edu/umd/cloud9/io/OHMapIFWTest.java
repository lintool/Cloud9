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

import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class OHMapIFWTest {

	@Test
	public void testBasic() throws IOException {
		OHMapIFW m = new OHMapIFW();

		m.put(2, 5.0f);
		m.put(1, 22.0f);

		float value;

		assertEquals(m.size(), 2);

		value = m.get(2);
		assertTrue(value == 5.0f);

		value = m.remove(2);
		assertEquals(m.size(), 1);

		value = m.get(1);
		assertTrue(value == 22.0f);
	}

	@Test
	public void testSerialize1() throws IOException {
		OHMapIFW m1 = new OHMapIFW();

		m1.put(3, 5.0f);
		m1.put(4, 22.0f);

		byte[] bytes = m1.serialize();
		OHMapIFW n2 = OHMapIFW.create(bytes);

		float value;

		assertEquals(n2.size(), 2);

		value = n2.get(3);
		assertTrue(value == 5.0f);

		value = n2.remove(3);
		assertEquals(n2.size(), 1);

		value = n2.get(4);
		assertTrue(value == 22.0f);
	}

	@Test
	public void testSerializeLazy1() throws IOException {
		OHMapIFW.setLazyDecodeFlag(true);
		OHMapIFW m1 = new OHMapIFW();

		m1.put(3, 5.0f);
		m1.put(4, 22.0f);

		byte[] bytes = m1.serialize();
		OHMapIFW m2 = OHMapIFW.create(bytes);

		assertFalse(m2.isDecoded());
		assertEquals(2, m2.size());

		int[] keys = m2.getKeys();
		float[] values = m2.getValues();

		assertTrue(keys[0] == 3);
		assertTrue(keys[1] == 4);

		assertTrue(values[0] == 5.0f);
		assertTrue(values[1] == 22.0f);

		m2.decode();
		assertTrue(m2.isDecoded());
		
		float value;
		assertEquals(m2.size(), 2);

		value = m2.get(3);
		assertTrue(value == 5.0f);

		value = m2.remove(3);
		assertEquals(1, m2.size());

		value = m2.get(4);
		assertTrue(value == 22.0f);
	}

	@Test
	public void testSerializeLazy2() throws IOException {
		OHMapIFW.setLazyDecodeFlag(true);
		OHMapIFW m1 = new OHMapIFW();

		m1.put(3, 5.0f);
		m1.put(4, 22.0f);

		byte[] bytes = m1.serialize();
		OHMapIFW m2 = OHMapIFW.create(bytes);

		assertFalse(m2.isDecoded());
		assertEquals(2, m2.size());

		int[] keys = m2.getKeys();
		float[] values = m2.getValues();

		assertTrue(keys[0] == 3);
		assertTrue(keys[1] == 4);

		assertTrue(values[0] == 5.0f);
		assertTrue(values[1] == 22.0f);

		m2.decode();
		float value;
		assertEquals(m2.size(), 2);

		value = m2.get(3);
		assertTrue(value == 5.0f);

		value = m2.remove(3);
		assertEquals(m2.size(), 1);

		value = m2.get(4);
		assertTrue(value == 22.0f);
	}

	@Test
	public void testLazyPlus1() throws IOException {
		OHMapIFW.setLazyDecodeFlag(true);
		
		OHMapIFW m1 = new OHMapIFW();
		m1.put(3, 5.0f);
		m1.put(4, 22.0f);

		byte[] bytes1 = m1.serialize();
		
		OHMapIFW m2 = new OHMapIFW();
		m2.put(3, 1.0f);
		m2.put(4, 1.0f);
		m2.put(5, 1.0f);

		byte[] bytes2 = m2.serialize();

		OHMapIFW n1 = OHMapIFW.create(bytes1);
		OHMapIFW n2 = OHMapIFW.create(bytes2);

		assertFalse(n1.isDecoded());
		assertEquals(2, n1.size());

		assertFalse(n2.isDecoded());
		assertEquals(3, n2.size());

		// n1 isn't decoded, n2 isn't decoded
		n1.plus(n2);

		assertTrue(n1.size() == 3);
		assertTrue(n1.get(3) == 6.0f);
		assertTrue(n1.get(4) == 23.0f);
		assertTrue(n1.get(5) == 1.0f);
		assertTrue(n1.isDecoded());
		assertFalse(n2.isDecoded());
	}
	
	@Test
	public void testLazyPlus2() throws IOException {
		OHMapIFW.setLazyDecodeFlag(true);
		
		OHMapIFW m1 = new OHMapIFW();
		m1.put(3, 5.0f);
		m1.put(4, 22.0f);

		byte[] bytes1 = m1.serialize();
		
		OHMapIFW m2 = new OHMapIFW();
		m2.put(3, 1.0f);
		m2.put(4, 1.0f);
		m2.put(5, 1.0f);

		byte[] bytes2 = m2.serialize();

		OHMapIFW n1 = OHMapIFW.create(bytes1);
		OHMapIFW n2 = OHMapIFW.create(bytes2);

		assertFalse(n1.isDecoded());
		assertEquals(2, n1.size());

		assertFalse(n2.isDecoded());
		assertEquals(3, n2.size());

		// n1 isn't decoded, n2 is
		n2.decode();
		n1.plus(n2);

		assertTrue(n1.size() == 3);
		assertTrue(n1.get(3) == 6.0f);
		assertTrue(n1.get(4) == 23.0f);
		assertTrue(n1.get(5) == 1.0f);
		assertTrue(n1.isDecoded());
		assertTrue(n2.isDecoded());
	}
	
	@Test
	public void testLazyPlus3() throws IOException {
		OHMapIFW.setLazyDecodeFlag(true);
		
		OHMapIFW m1 = new OHMapIFW();
		m1.put(3, 5.0f);
		m1.put(4, 22.0f);

		byte[] bytes1 = m1.serialize();
		
		OHMapIFW m2 = new OHMapIFW();
		m2.put(3, 1.0f);
		m2.put(4, 1.0f);
		m2.put(5, 1.0f);

		byte[] bytes2 = m2.serialize();

		OHMapIFW n1 = OHMapIFW.create(bytes1);
		OHMapIFW n2 = OHMapIFW.create(bytes2);

		assertFalse(n1.isDecoded());
		assertEquals(2, n1.size());

		assertFalse(n2.isDecoded());
		assertEquals(3, n2.size());

		// n2 isn't decoded, n1 is
		n1.decode();
		n1.plus(n2);

		assertTrue(n1.size() == 3);
		assertTrue(n1.get(3) == 6.0f);
		assertTrue(n1.get(4) == 23.0f);
		assertTrue(n1.get(5) == 1.0f);
		assertTrue(n1.isDecoded());
		assertFalse(n2.isDecoded());
	}
	
	@Test
	public void testLazyPlus4() throws IOException {
		OHMapIFW.setLazyDecodeFlag(true);
		
		OHMapIFW m1 = new OHMapIFW();
		m1.put(3, 5.0f);
		m1.put(4, 22.0f);

		byte[] bytes1 = m1.serialize();
		
		OHMapIFW m2 = new OHMapIFW();
		m2.put(3, 1.0f);
		m2.put(4, 1.0f);
		m2.put(5, 1.0f);

		byte[] bytes2 = m2.serialize();

		OHMapIFW n1 = OHMapIFW.create(bytes1);
		OHMapIFW n2 = OHMapIFW.create(bytes2);

		assertFalse(n1.isDecoded());
		assertEquals(2, n1.size());

		assertFalse(n2.isDecoded());
		assertEquals(3, n2.size());

		// both n1 and n2 are decoded
		n1.decode();
		n2.decode();
		n1.plus(n2);

		assertTrue(n1.size() == 3);
		assertTrue(n1.get(3) == 6.0f);
		assertTrue(n1.get(4) == 23.0f);
		assertTrue(n1.get(5) == 1.0f);
		assertTrue(n1.isDecoded());
		assertTrue(n2.isDecoded());
	}
	
	@Test
	public void testSerializeEmpty() throws IOException {
		OHMapIFW m1 = new OHMapIFW();

		// make sure this does nothing
		m1.decode();

		assertTrue(m1.size() == 0);

		byte[] bytes = m1.serialize();
		OHMapIFW m2 = OHMapIFW.create(bytes);

		assertTrue(m2.size() == 0);
	}

	// TODO: Should add a test case for lazy add

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(OHMapIFWTest.class);
	}

}
