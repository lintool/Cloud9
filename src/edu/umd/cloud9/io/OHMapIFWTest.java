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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
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

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m1.write(dataOut);

		OHMapIFW n2 = OHMapIFW.create(new DataInputStream(new ByteArrayInputStream(bytesOut
				.toByteArray())));

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

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m1.write(dataOut);

		OHMapIFW n2 = OHMapIFW.create(new DataInputStream(new ByteArrayInputStream(bytesOut
				.toByteArray())));

		assertEquals(0, n2.size());
		
		int[] keys = n2.getKeys();
		float[] values = n2.getValues();

		assertTrue(keys[0] == 3);
		assertTrue(keys[1] == 4);

		assertTrue(values[0] == 5.0f);
		assertTrue(values[1] == 22.0f);

		n2.decode();
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
	public void testSerializeEmpty() throws IOException {
		OHMapIFW m1 = new OHMapIFW();

		// make sure this does nothing
		m1.decode();
		
		assertTrue(m1.size() == 0);

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		m1.write(dataOut);

		OHMapIFW m2 = OHMapIFW.create(new DataInputStream(new ByteArrayInputStream(bytesOut
				.toByteArray())));

		assertTrue(m2.size() == 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(OHMapIFWTest.class);
	}

}
