package edu.umd.cloud9.integration.collection.clue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.junit.Test;

import com.google.common.base.Joiner;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.clue.ClueWarcDocnoMapping;
import edu.umd.cloud9.collection.clue.ClueWarcForwardIndex;
import edu.umd.cloud9.integration.IntegrationUtils;

public class IntegrationTest {
  private static final Random random = new Random();

  private static final Path collectionPathRepacked =
      new Path("/shared/collections/ClueWeb09/collection.compressed.block/en.01");
  private static final Path collectionPathRaw =
      new Path("/shared/collections/ClueWeb09/collection.raw/");
  private static final String tmpPrefix = "tmp-" + IntegrationTest.class.getCanonicalName() + "-"
      + random.nextInt(10000);

  private static final String mappingFile = tmpPrefix + "-mapping.dat";

  @Test
  public void runTests() throws Exception {
    testDocnoMapping();
    testDemoCountDocsRepacked();
    testDemoCountDocsRaw();
    testForwardIndex();
  }

  private void testDocnoMapping() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPathRepacked));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.collection.clue.ClueWarcDocnoMappingBuilder.class.getCanonicalName(),
        "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + collectionPathRepacked,
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + mappingFile };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    ClueWarcDocnoMapping mapping = new ClueWarcDocnoMapping();
    mapping.loadMapping(new Path(mappingFile), fs);

    assertEquals("clueweb09-en0000-00-00000", mapping.getDocid(1));
    assertEquals("clueweb09-en0000-29-13313", mapping.getDocid(1000000));

    assertEquals(1, mapping.getDocno("clueweb09-en0000-00-00000"));
    assertEquals(1000000, mapping.getDocno("clueweb09-en0000-29-13313"));
  }

  private void testDemoCountDocsRepacked() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPathRepacked));

    String records = tmpPrefix + "-records.txt";

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"), 
        edu.umd.cloud9.collection.clue.CountClueWarcRecords.class.getCanonicalName(),
        "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-repacked",
        "-path=" + collectionPathRepacked,
        "-docnoMapping=" + mappingFile,
        "-countOutput=" + records};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    LineReader reader = new LineReader(fs.open(new Path(records)));
    Text str = new Text();
    reader.readLine(str);
    reader.close();

    assertEquals(50220423, Integer.parseInt(str.toString()));
  }

  private void testDemoCountDocsRaw() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPathRaw));

    String records = tmpPrefix + "-records.txt";

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"), 
        edu.umd.cloud9.collection.clue.CountClueWarcRecords.class.getCanonicalName(),
        "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-original",
        "-segment=1",
        "-path=" + collectionPathRaw,
        "-docnoMapping=" + mappingFile,
        "-countOutput=" + records};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    LineReader reader = new LineReader(fs.open(new Path(records)));
    Text str = new Text();
    reader.readLine(str);
    reader.close();

    assertEquals(50220423, Integer.parseInt(str.toString()));
  }

  private void testForwardIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPathRepacked));

    String index = tmpPrefix + "-findex.dat";

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"), 
        edu.umd.cloud9.collection.clue.ClueWarcForwardIndexBuilder.class.getCanonicalName(),
        "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-collection=" + collectionPathRepacked,
        "-index=" + index };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    ClueWarcForwardIndex findex = new ClueWarcForwardIndex();
    findex.loadIndex(new Path(index), new Path(mappingFile), fs);

    assertTrue(findex.getDocument(14069750).getContent()
        .contains("Vizergy: How Design and SEO work together"));
    assertTrue(findex.getDocument("clueweb09-en0008-76-19728").getContent()
        .contains("Jostens - Homeschool Yearbooks"));
    assertEquals(1, findex.getFirstDocno());
    assertEquals(50220423, findex.getLastDocno());
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(IntegrationTest.class);
  }
}
