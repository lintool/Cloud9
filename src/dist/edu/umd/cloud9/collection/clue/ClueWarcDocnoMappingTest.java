package edu.umd.cloud9.collection.clue;

import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class ClueWarcDocnoMappingTest {

	@Test
	public void testSerialize1() throws Exception {
		ClueWarcDocnoMapping mapping = new ClueWarcDocnoMapping();

		assertEquals(1, mapping.getDocno("clueweb09-en0000-00-00000"));

		assertEquals(28335180, mapping.getDocno("clueweb09-en0007-91-00000"));
		assertEquals(28378418, mapping.getDocno("clueweb09-en0007-91-43238"));
		assertEquals(28378419, mapping.getDocno("clueweb09-en0007-92-00000"));

		assertEquals(44262895, mapping.getDocno("clueweb09-enwp00-00-00000"));
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(ClueWarcDocnoMappingTest.class);
	}

}
