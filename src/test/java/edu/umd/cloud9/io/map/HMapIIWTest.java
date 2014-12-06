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

package edu.umd.cloud9.io.map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.io.map.HMapIFW;
import edu.umd.cloud9.io.map.HMapIIW;

public class HMapIIWTest {

	@Test
	public void testBasic() throws IOException {
		HMapIIW m = new HMapIIW();

		m.put(2, 5);
		m.put(1, 22);

		int value;

		assertEquals(m.size(), 2);

		value = m.get(2);
		assertEquals(5, value);

		value = m.remove(2);
		assertEquals(m.size(), 1);

		value = m.get(1);
		assertEquals(22, value);
	}

	@Test
	public void testSerialize1() throws IOException {
		HMapIIW.setLazyDecodeFlag(false);
		HMapIIW m1 = new HMapIIW();

		m1.put(3, 5);
		m1.put(4, 22);

		HMapIIW n2 = HMapIIW.create(m1.serialize());

		int value;

		assertEquals(n2.size(), 2);

		value = n2.get(3);
		assertEquals(5, value);

		value = n2.remove(3);
		assertEquals(n2.size(), 1);

		value = n2.get(4);
		assertEquals(value, 22);
	}

	@Test
	public void testSerializeLazy1() throws IOException {
		HMapIIW.setLazyDecodeFlag(true);
		HMapIIW m1 = new HMapIIW();

		m1.put(3, 5);
		m1.put(4, 22);

		HMapIIW m2 = HMapIIW.create(m1.serialize());

		assertEquals(2, m2.size());

		int[] keys = m2.getKeys();
		int[] values = m2.getValues();

		assertTrue(keys[0] == 3);
		assertTrue(keys[1] == 4);

		assertTrue(values[0] == 5.0f);
		assertTrue(values[1] == 22.0f);

		assertFalse(m2.isDecoded());
		assertEquals(m2.size(), 2);

		m2.decode();
		assertTrue(m2.isDecoded());

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
	public void testSerializeEmpty() throws IOException {
		HMapIIW m1 = new HMapIIW();

		// make sure this does nothing
		m1.decode();

		assertTrue(m1.size() == 0);

		HMapIFW m2 = HMapIFW.create(m1.serialize());

		assertTrue(m2.size() == 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HMapIIWTest.class);
	}

}
