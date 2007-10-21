package edu.umd.cloud9.tuple;

import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

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

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(SchemaTest.class);
	}

}
