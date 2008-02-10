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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class TupleTest {

	public static final Schema SCHEMA1 = new Schema();
	static {
		SCHEMA1.addField("field0", String.class, "default");
		SCHEMA1.addField("field1", Boolean.class, true);
		SCHEMA1.addField("field2", Integer.class, new Integer(1));
		SCHEMA1.addField("field3", Long.class, new Long(2));
		SCHEMA1.addField("field4", Float.class, new Float(2.5));
		SCHEMA1.addField("field5", Double.class, new Double(3.14));
		SCHEMA1.addField("field6", String.class, "test");
	}

	// next few cases tests invalid field names: should throw exceptions
	@Test(expected = TupleException.class)
	public void testAccessNonExistentField1() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.get("FIELD");
	}

	@Test(expected = TupleException.class)
	public void testAccessNonExistentField2() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.getSymbol("FIELD");
	}

	@Test(expected = TupleException.class)
	public void testAccessNonExistentField3() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.set("Field0", "test");
	}

	@Test(expected = TupleException.class)
	public void testAccessNonExistentField4() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.setSymbol("Field0", "test");
	}

	@Test(expected = TupleException.class)
	public void testAccessNonExistentField5() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.containsSymbol("Field0");
	}

	@Test(expected = TupleException.class)
	public void testAccessNonExistentField6() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.getFieldType("Field0");
	}

	// can't set fields to null
	@Test(expected = TupleException.class)
	public void testSetNull1() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.set(0, null);
	}

	// can't set symbol as null
	@Test(expected = TupleException.class)
	public void testSetNull2() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.setSymbol(0, null);
	}

	// mismatch in field type
	@Test(expected = TupleException.class)
	public void testSetWrongType() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.set(0, 1);
	}

	@Test
	public void testHashCode() {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.setSymbol(0, "*");
		int hash1 = tuple.hashCode();

		tuple.set(0, "sample");
		int hash2 = tuple.hashCode();

		assertTrue(hash1 != hash2);
	}
	
	@Test
	public void testSorting() {
		
		Tuple tuple1 = SCHEMA1.instantiate();
		Tuple tuple2 = SCHEMA1.instantiate();
		
		assertEquals(tuple1.compareTo(tuple2), 0);
		
		tuple1.set("field0", "a");
		tuple1.set("field0", "b");
		
		assertTrue(tuple1.compareTo(tuple2) < 0);
		assertTrue(tuple2.compareTo(tuple1) > 0);
		
		tuple1.set("field1", true);
		tuple1.set("field1", false);
		
		assertTrue(tuple1.compareTo(tuple2) < 0);
		assertTrue(tuple2.compareTo(tuple1) > 0);
		
		tuple1.set("field2", 1);
		tuple1.set("field2", 2);
		
		assertTrue(tuple1.compareTo(tuple2) < 0);
		assertTrue(tuple2.compareTo(tuple1) > 0);
		
		tuple1.set("field3", 1L);
		tuple1.set("field3", 2L);
		
		assertTrue(tuple1.compareTo(tuple2) < 0);
		assertTrue(tuple2.compareTo(tuple1) > 0);

		tuple1.set("field4", 1.0f);
		tuple1.set("field4", 2.0f);
		
		assertTrue(tuple1.compareTo(tuple2) < 0);
		assertTrue(tuple2.compareTo(tuple1) > 0);

		tuple1.set("field5", 1.0d);
		tuple1.set("field5", 2.0d);
		
		assertTrue(tuple1.compareTo(tuple2) < 0);
		assertTrue(tuple2.compareTo(tuple1) > 0);
	}

	// tests unpacking of default values
	@Test
	public void testSerializeDefaultValues() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		tuple.write(dataOut);

		Tuple t = Tuple.createFrom(new DataInputStream(
				new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(t.get(0), "default");
		assertEquals(t.get(1), true);
		assertEquals(t.get(2), new Integer(1));
		assertEquals(t.get(3), new Long(2));
		assertEquals(t.get(4), new Float(2.5));
		assertEquals(t.get(5), new Double(3.14));
		assertEquals(t.get(6), "test");

		assertEquals(t.get("field0"), "default");
		assertEquals(t.get("field1"), true);
		assertEquals(t.get("field2"), new Integer(1));
		assertEquals(t.get("field3"), new Long(2));
		assertEquals(t.get("field4"), new Float(2.5));
		assertEquals(t.get("field5"), new Double(3.14));
		assertEquals(t.get("field6"), "test");

		assertEquals(t.getFieldType(0), String.class);
		assertEquals(t.getFieldType(1), Boolean.class);
		assertEquals(t.getFieldType(2), Integer.class);
		assertEquals(t.getFieldType(3), Long.class);
		assertEquals(t.getFieldType(4), Float.class);
		assertEquals(t.getFieldType(5), Double.class);
		assertEquals(t.getFieldType(6), String.class);
	}

	@Test
	public void testSerializeInstantiatedValues() throws IOException {
		Tuple tuple = SCHEMA1.instantiate("Hello world!", false,
				new Integer(5), new Long(3), new Float(1.2), new Double(2.871),
				"another string");

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		tuple.write(dataOut);

		Tuple t = Tuple.createFrom(new DataInputStream(
				new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(t.get(0), "Hello world!");
		assertEquals(t.get(1), false);
		assertEquals(t.get(2), new Integer(5));
		assertEquals(t.get(3), new Long(3));
		assertEquals(t.get(4), new Float(1.2));
		assertEquals(t.get(5), new Double(2.871));
		assertEquals(t.get(6), "another string");
	}

	@Test
	public void testSerializeSetValues() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.set(0, "Hello world!");
		tuple.set(1, false);
		tuple.set(2, new Integer(5));
		tuple.set(3, new Long(3));
		tuple.set(4, new Float(1.2));
		tuple.set(5, new Double(2.871));
		tuple.set(6, "another string");

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		tuple.write(dataOut);

		Tuple t = Tuple.createFrom(new DataInputStream(
				new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(t.get(0), "Hello world!");
		assertEquals(t.get(1), false);
		assertEquals(t.get(2), new Integer(5));
		assertEquals(t.get(3), new Long(3));
		assertEquals(t.get(4), new Float(1.2));
		assertEquals(t.get(5), new Double(2.871));
		assertEquals(t.get(6), "another string");
	}

	@Test
	public void testToString() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		assertEquals(tuple.toString(), "(default, true, 1, 2, 2.5, 3.14, test)");
	}

	@Test
	public void testSymbols() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		assertFalse(tuple.containsSymbol(0));
		assertFalse(tuple.containsSymbol(1));
		assertFalse(tuple.containsSymbol(2));
		assertFalse(tuple.containsSymbol(3));
		assertFalse(tuple.containsSymbol(4));
		assertFalse(tuple.containsSymbol(5));
		assertFalse(tuple.containsSymbol(6));

		assertFalse(tuple.containsSymbol("field0"));
		assertFalse(tuple.containsSymbol("field1"));
		assertFalse(tuple.containsSymbol("field2"));
		assertFalse(tuple.containsSymbol("field3"));
		assertFalse(tuple.containsSymbol("field4"));
		assertFalse(tuple.containsSymbol("field5"));
		assertFalse(tuple.containsSymbol("field6"));

		tuple.setSymbol(0, "*");
		tuple.setSymbol("field1", "*");

		assertTrue(tuple.containsSymbol(0));
		assertTrue(tuple.containsSymbol(1));
		assertFalse(tuple.containsSymbol(2));
		assertFalse(tuple.containsSymbol(3));
		assertFalse(tuple.containsSymbol(4));
		assertFalse(tuple.containsSymbol(5));
		assertFalse(tuple.containsSymbol(6));

		assertEquals(tuple.get(0), null);
		assertEquals(tuple.getSymbol(0), "*");

		assertEquals(tuple.get(1), null);
		assertEquals(tuple.getSymbol(1), "*");

		assertEquals(tuple.getFieldType(0), String.class);
		assertEquals(tuple.getFieldType(1), Boolean.class);

		assertEquals(tuple.toString(), "(*, *, 1, 2, 2.5, 3.14, test)");
	}

	@Test
	public void testSerializeSymbols() throws IOException {
		Tuple tuple = SCHEMA1.instantiate();

		tuple.setSymbol(0, "*");
		tuple.setSymbol("field1", "*");

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		tuple.write(dataOut);

		Tuple t = Tuple.createFrom(new DataInputStream(
				new ByteArrayInputStream(bytesOut.toByteArray())));

		assertTrue(t.containsSymbol(0));
		assertTrue(t.containsSymbol(1));
		assertFalse(t.containsSymbol(2));
		assertFalse(t.containsSymbol(3));
		assertFalse(t.containsSymbol(4));
		assertFalse(t.containsSymbol(5));
		assertFalse(t.containsSymbol(6));

		assertEquals(t.get(0), null);
		assertEquals(t.getSymbol(0), "*");

		assertEquals(t.get(1), null);
		assertEquals(t.getSymbol(1), "*");

		assertEquals(tuple.getFieldType(0), String.class);
		assertEquals(tuple.getFieldType(1), Boolean.class);
	}

	public static final Schema SCHEMA2 = new Schema();
	static {
		SCHEMA2.addField("field0", Integer.class, 0);
		SCHEMA2.addField("field1", IntWritable.class, new IntWritable(0));
		SCHEMA2.addField("field2", Text.class, new Text("default"));
	}

	@Test
	public void testSerializeWritableFields() throws IOException {
		Tuple tuple = SCHEMA2.instantiate();

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		tuple.write(dataOut);

		Tuple t = Tuple.createFrom(new DataInputStream(
				new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals(t.get(0), 0);
		assertEquals(t.get(1), new IntWritable(0));
		assertEquals(t.get(2), new Text("default"));

		assertEquals(t.getFieldType(0), Integer.class);
		assertEquals(t.getFieldType(1), IntWritable.class);
		assertEquals(t.getFieldType(2), Text.class);
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(TupleTest.class);
	}

}
