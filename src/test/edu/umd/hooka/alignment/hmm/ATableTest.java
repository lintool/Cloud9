package edu.umd.hooka.alignment.hmm;

import org.junit.Test;

public class ATableTest {
/*
	@Test
	public void testNormalize() {
		ATable at = new ATable(true, 1, 10);
		at.add(1, 'a', 99, 1.0f);
		at.add(-1, 'a', 99, 0.5f);
		at.add(0, 'a', 99, 0.5f);
		at.normalize();
		assertEquals(0.25f, at.get(0, 'a'), 0.01f);
		assertEquals(0.5f, at.get(1, 'a'), 0.01f);
	}

	@Test
	public void testReadFields() {
		try {
			File temp = File.createTempFile("ttable", null);
			temp.deleteOnExit();

			DataOutputStream dos = new DataOutputStream(
				new FileOutputStream(temp));
			ATable at = new ATable(true, 1, 10);
			at.add(1, 'a', 99, 1.0f);
			at.add(-1, 'a', 99, 0.5f);
			at.add(0, 'a', 99, 0.5f);
			at.normalize();
			assertEquals(0.25f, at.get(0, 'a'), 0.01f);
			assertEquals(0.5f, at.get(1, 'a'), 0.01f);
			System.err.println(at);
			at.write(dos);
			dos.close();
			System.err.println("Size of at on disk: " + dos.size());
			DataInputStream dis = new DataInputStream(
					new FileInputStream(temp));
			ATable at2 = new ATable();
			at2.readFields(dis);
			System.err.println(at2);
			dis.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Caught "+e);
		}
	}
	
	@Test
	public void testDistribution() {
		ATable at = new ATable(true, 1, 100);
		at.normalize();
		System.out.println(at);
	}
	*/
	@Test
	public void testNonHomogeneous() {
		ATable at = new ATable(false, 10, 10);
		at.normalize();
		System.out.println(at);
	}

}
