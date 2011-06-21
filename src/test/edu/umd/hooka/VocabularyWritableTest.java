package edu.umd.hooka;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

public class VocabularyWritableTest extends TestCase {

	VocabularyWritable v;
	int hello;
	int goodbye;
	int bar;
	
	protected void setUp() throws Exception {
		super.setUp();
		v = new VocabularyWritable();
		hello = v.addOrGet("hello");
		goodbye = v.addOrGet("Goodbye");
		bar = v.addOrGet("bar");
	}

	public void testSize() {
		assertEquals(v.size(), 4);
		v.addOrGet("foo");
		assertEquals(v.size(), 5);
	}

	public void testAddOrGet() {
		String baz = "baz";
		int i = v.addOrGet(baz);
		assertEquals(v.get(i), baz);
		String h = "h";
		String ello = h + "ello";
		assertEquals(v.addOrGet(ello), hello);
	}

	public void testGetString() {
		String a = "Good";
		String b = a + "bye";
		assertEquals(v.get(b), goodbye);
	}

	public void testGetInt() {
		assertEquals(v.get(bar), "bar");
	}

	public void testReadFields() {
		try {
			File temp = File.createTempFile("phrpr", null);
			temp.deleteOnExit();

			DataOutputStream dos = new DataOutputStream(
				new FileOutputStream(temp));
			v.write(dos);
			dos.close();
			System.err.println("Size of voc on disk: " + dos.size());
			DataInputStream dis = new DataInputStream(
					new FileInputStream(temp));
			VocabularyWritable vw = new VocabularyWritable();
			vw.readFields(dis);
			assertEquals(v.get(bar), vw.get(bar));
			dis.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Caught "+e);
		}
	}


}
