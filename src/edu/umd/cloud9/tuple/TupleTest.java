package edu.umd.cloud9.tuple;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.io.BytesWritable;
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

	// tests unpacking of default values
	@Test
	public void testDefaultValues() throws IOException {
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
	}

	@Test
	public void testInstantiatedValues() throws IOException {
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
	public void testSetValues() throws IOException {
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

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(TupleTest.class);
	}

}
