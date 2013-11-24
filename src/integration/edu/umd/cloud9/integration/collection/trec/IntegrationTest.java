package edu.umd.cloud9.integration.collection.trec;

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
import edu.umd.cloud9.collection.trec.TrecDocnoMapping;
import edu.umd.cloud9.collection.trec.TrecForwardIndex;
import edu.umd.cloud9.integration.IntegrationUtils;

public class IntegrationTest {
  private static final Random random = new Random();

  private static final Path collectionPath = new Path("/shared/collections/trec/trec4-5_noCRFR.xml");
  private static final String tmpPrefix = "tmp-" + IntegrationTest.class.getCanonicalName() +
      "-" + random.nextInt(10000);

  private static final String mappingFile = tmpPrefix + "-mapping.dat";

  @Test
  public void runTests() throws Exception {
    testDocnoMapping();
    testDemoCountDocs();
    testForwardIndex();
  }

  private void testDocnoMapping() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.collection.trec.TrecDocnoMappingBuilder.class.getCanonicalName(),
        "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + collectionPath,
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + mappingFile };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    TrecDocnoMapping mapping = new TrecDocnoMapping();
    mapping.loadMapping(new Path(mappingFile), fs);

    assertEquals("FBIS3-1", mapping.getDocid(1));
    assertEquals("LA061490-0139", mapping.getDocid(400000));

    assertEquals(1, mapping.getDocno("FBIS3-1"));
    assertEquals(400000, mapping.getDocno("LA061490-0139"));
  }

  private void testDemoCountDocs() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    String output = tmpPrefix + "-cnt";
    String records = tmpPrefix + "-records.txt";

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.collection.trec.CountTrecDocuments.class.getCanonicalName(),
        "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-collection=" + collectionPath,
        "-output=" + output,
        "-docnoMapping=" + mappingFile,
        "-countOutput=" + records};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    LineReader reader = new LineReader(fs.open(new Path(records)));
    Text str = new Text();
    reader.readLine(str);
    reader.close();

    assertEquals(472525, Integer.parseInt(str.toString()));
  }

  private void testForwardIndex() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    String index = tmpPrefix + "-findex.dat";
    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.collection.trec.TrecForwardIndexBuilder.class.getCanonicalName(),
        "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-collection=" + collectionPath,
        "-index=" + index,
        "-docnoMapping=" + mappingFile };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    TrecForwardIndex findex = new TrecForwardIndex();
    findex.loadIndex(new Path(index), new Path(mappingFile), fs);

    assertTrue(findex.getDocument(1).getContent().contains("Newspapers in the Former Yugoslav Republic"));
    assertTrue(findex.getDocument("FBIS3-1").getContent().contains("Newspapers in the Former Yugoslav Republic"));
    assertEquals(1, findex.getFirstDocno());
    assertEquals(472525, findex.getLastDocno());
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(IntegrationTest.class);
  }
}
