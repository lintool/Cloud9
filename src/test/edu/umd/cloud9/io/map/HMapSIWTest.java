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
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.io.map.HMapSIW;

public class HMapSIWTest {

	@Test
	public void testBasic() throws IOException {
		HMapSIW m = new HMapSIW();

		m.put("hi", 5);
		m.put("there", 22);

		String key;
		int value;

		assertEquals(m.size(), 2);

		key = "hi";
		value = m.get(key);
		assertTrue(value == 5);

		value = m.remove(key);
		assertEquals(m.size(), 1);

		key = "there";
		value = m.get(key);
		assertTrue(value == 2);
	}

	@Test
	public void testSerialize1() throws IOException {
		HMapSIW m1 = new HMapSIW();

		m1.put("hi", 5);
		m1.put("there", 22);

		HMapSIW n2 = HMapSIW.create(m1.serialize());

		String key;
		float value;

		assertEquals(n2.size(), 2);

		key = "hi";
		value = n2.get(key);
		assertTrue(value == 5);

		value = n2.remove(key);
		assertEquals(n2.size(), 1);

		key = "there";
		value = n2.get(key);
		assertTrue(value == 22);
	}

	@Test
	public void testSerializeEmpty() throws IOException {
		HMapSIW m1 = new HMapSIW();

		assertTrue(m1.size() == 0);

		HMapSIW m2 = HMapSIW.create(m1.serialize());

		assertTrue(m2.size() == 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HMapSFWTest.class);
	}

}
