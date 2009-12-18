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

import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class OHMapIVWTest {

	@Test
	public void testBasic() throws IOException {
		OHMapIVW<Text> m = new OHMapIVW<Text>();

		m.put(5, new Text("hi"));
		m.put(22, new Text("there"));

		assertEquals(m.size(), 2);

		Text value = m.get(5);
		assertEquals(new Text("hi"), value);

		value = m.remove(5);
		assertEquals(new Text("hi"), value);
		assertEquals(m.size(), 1);

		value = m.get(22);
		assertEquals(new Text("there"), value);
	}

	@Test
	public void testSerialize1() throws IOException {
		OHMapIVW<Text> m1 = new OHMapIVW<Text>();

		m1.put(5, new Text("hi"));
		m1.put(22, new Text("there"));

		OHMapIVW<Text> m2 = OHMapIVW.<Text> create(m1.serialize());

		assertEquals(m2.size(), 2);

		Text value = m2.get(5);
		assertEquals(new Text("hi"), value);

		value = m2.remove(5);
		assertEquals(new Text("hi"), value);
		assertEquals(m2.size(), 1);

		value = m2.get(22);
		assertEquals(new Text("there"), value);
	}

	@Test
	public void testSerializeEmpty() throws IOException {
		OHMapIVW<Text> m1 = new OHMapIVW<Text>();

		assertTrue(m1.size() == 0);

		OHMapIVW<Text> m2 = OHMapIVW.<Text> create(m1.serialize());

		assertTrue(m2.size() == 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(OHMapIVWTest.class);
	}

}
