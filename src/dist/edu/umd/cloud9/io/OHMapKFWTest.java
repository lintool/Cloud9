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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.junit.Test;

public class OHMapKFWTest {

	@Test
	public void testBasic() throws IOException {
		OHMapKFW<Text> m = new OHMapKFW<Text>();

		m.put(new Text("hi"), 5.0f);
		m.put(new Text("there"), 22.0f);

		Text key;
		float value;

		assertEquals(m.size(), 2);

		key = new Text("hi");
		value = m.get(key);
		assertTrue(value == 5.0f);

		value = m.remove(key);
		assertEquals(m.size(), 1);

		key = new Text("there");
		value = m.get(key);
		assertTrue(value == 22.0f);
	}

	@Test
	public void testSerialize1() throws IOException {
		OHMapKFW<Text> m1 = new OHMapKFW<Text>();

		m1.put(new Text("hi"), 5.0f);
		m1.put(new Text("there"), 22.0f);

		OHMapKFW<Text> n2 = OHMapKFW.<Text> create(m1.serialize());

		Text key;
		float value;

		assertEquals(n2.size(), 2);

		key = new Text("hi");
		value = n2.get(key);
		assertTrue(value == 5.0f);

		value = n2.remove(key);
		assertEquals(n2.size(), 1);

		key = new Text("there");
		value = n2.get(key);
		assertTrue(value == 22.0f);
	}

	@Test(expected = IOException.class)
	public void testTypeSafety() throws IOException {
		OHMapKFW<WritableComparable<?>> m1 = new OHMapKFW<WritableComparable<?>>();

		m1.put(new Text("hi"), 4.0f);
		m1.put(new IntWritable(0), 76.0f);

		OHMapKFW<Text> m2 = OHMapKFW.<Text> create(m1.serialize());

		m2.size();
	}

	@Test
	public void testSerializeEmpty() throws IOException {
		OHMapKFW<WritableComparable<?>> m1 = new OHMapKFW<WritableComparable<?>>();

		assertTrue(m1.size() == 0);

		OHMapKFW<Text> m2 = OHMapKFW.<Text> create(m1.serialize());

		assertTrue(m2.size() == 0);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(OHMapKFWTest.class);
	}

}
