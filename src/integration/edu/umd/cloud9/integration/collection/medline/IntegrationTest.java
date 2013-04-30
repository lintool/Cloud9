package edu.umd.cloud9.integration.collection.medline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.base.Joiner;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.medline.MedlineDocnoMapping;
import edu.umd.cloud9.integration.IntegrationUtils;

public class IntegrationTest {
  private static final Random random = new Random();

  private static final Path collectionPath = new Path("/shared/collections/medline04");
  private static final String tmpPrefix = "tmp-" + IntegrationTest.class.getCanonicalName() +
      "-" + random.nextInt(10000);

  private static final String mappingFile = tmpPrefix + "-mapping.dat";

  @Test
  public void testDocnoMapping() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.collection.medline.MedlineDocnoMappingBuilder.class.getCanonicalName(),
        "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-" + DocnoMapping.BuilderUtils.COLLECTION_OPTION + "=" + collectionPath,
        "-" + DocnoMapping.BuilderUtils.MAPPING_OPTION + "=" + mappingFile };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    MedlineDocnoMapping mapping = new MedlineDocnoMapping();
    mapping.loadMapping(new Path(mappingFile), fs);

    assertEquals("45641", mapping.getDocid(1));
    assertEquals("45740", mapping.getDocid(100));
    assertEquals("7472623", mapping.getDocid(10000));
    assertEquals("8739651", mapping.getDocid(1000000)); 

    assertEquals(1, mapping.getDocno("45641"));
    assertEquals(100, mapping.getDocno("45740"));
    assertEquals(10000, mapping.getDocno("7472623"));
    assertEquals(1000000, mapping.getDocno("8739651"));
  }

  @Test
  public void testDemoCountDocs() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    String output = tmpPrefix + "-cnt";

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.collection.medline.CountMedlineCitations.class.getCanonicalName(),
        "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-collection=" + collectionPath,
        "-output=" + output,
        "-docnoMapping=" + mappingFile };

    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(IntegrationTest.class);
  }
}
