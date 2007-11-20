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

package edu.umd.cloud9.tuple;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

public class SchemaTest {

	public static final Schema SCHEMA1 = new Schema();
	static {
		SCHEMA1.addField("field1", String.class, "default");
		SCHEMA1.addField("field2", Integer.class, new Integer(1));
	}

	@Test
	public void test1() {
		Tuple tuple = SCHEMA1.instantiate();

		assertEquals(tuple.get(0), "default");
		assertEquals(tuple.get(1), new Integer(1));

		assertEquals(tuple.get("field1"), "default");
		assertEquals(tuple.get("field2"), new Integer(1));
	}

	@Test
	public void test2() {
		Tuple tuple = SCHEMA1.instantiate("Hello world!", new Integer(5));
		assertEquals(tuple.get(0), "Hello world!");
		assertEquals(tuple.get(1), new Integer(5));
	}

	@Test(expected = SchemaException.class)
	public void testIllegalFieldsException1() {
		Schema schema = new Schema();
		schema.addField("field0", Integer.class, 0);
		schema.addField("field1", HashMap.class, null);
	}

	@Test(expected = SchemaException.class)
	public void testIllegalFieldsException2() {
		Schema schema = new Schema();
		schema.addField("field0", Integer.class, 0);
		// throws exception because Writable isn't a concrete class
		schema.addField("field1", Writable.class, null);
	}

	@Test
	public void testWritableFields() {
		Schema schema = new Schema();
		schema.addField("field0", Integer.class, 0);
		schema.addField("field1", IntWritable.class, new IntWritable(0));
		schema.addField("field2", Text.class, new Text("default"));

		Tuple t = schema.instantiate();
		assertEquals(t.get(0), 0);
		assertEquals(t.get(1), new IntWritable(0));
		assertEquals(t.get(2), new Text("default"));
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(SchemaTest.class);
	}

}
