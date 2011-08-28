package edu.umd.cloud9.collection.clue;

import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

public class ClueWarcDocnoMappingTest {

  @Test
  public void testDocidToDocno() throws Exception {
    ClueWarcDocnoMapping mapping = new ClueWarcDocnoMapping();

    mapping.loadMapping(new Path("etc/Clue-English-docno.mapping"),
        FileSystem.get(new Configuration()));

    assertEquals(1, mapping.getDocno("clueweb09-en0000-00-00000"));
    assertEquals(28335180, mapping.getDocno("clueweb09-en0007-91-00000"));
    assertEquals(28378418, mapping.getDocno("clueweb09-en0007-91-43238"));
    assertEquals(28378419, mapping.getDocno("clueweb09-en0007-92-00000"));
    assertEquals(44262895, mapping.getDocno("clueweb09-enwp00-00-00000"));
    assertEquals(50220424, mapping.getDocno("clueweb09-en0012-00-00000"));
    assertEquals(140950355, mapping.getDocno("clueweb09-en0038-18-42833"));
    assertEquals(503903810, mapping.getDocno("clueweb09-en0133-99-55739"));
  }

  @Test
  public void testDocnoToDocid() throws Exception {
    ClueWarcDocnoMapping mapping = new ClueWarcDocnoMapping();

    mapping.loadMapping(new Path("etc/Clue-English-docno.mapping"),
        FileSystem.get(new Configuration()));

    assertEquals("clueweb09-en0000-00-00000", mapping.getDocid(1));
    assertEquals("clueweb09-en0007-91-00000", mapping.getDocid(28335180));
    assertEquals("clueweb09-en0007-91-43238", mapping.getDocid(28378418));
    assertEquals("clueweb09-en0007-92-00000", mapping.getDocid(28378419));
    assertEquals("clueweb09-enwp00-00-00000", mapping.getDocid(44262895));
    assertEquals("clueweb09-en0012-00-00000", mapping.getDocid(50220424));
    assertEquals("clueweb09-en0038-18-42833", mapping.getDocid(140950355));
    assertEquals("clueweb09-en0133-99-55739", mapping.getDocid(503903810));
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ClueWarcDocnoMappingTest.class);
  }
}
