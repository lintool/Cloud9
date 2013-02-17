package edu.umd.cloud9.integration.example.ir;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.google.common.base.Joiner;

import edu.umd.cloud9.integration.IntegrationUtils;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class InvertedIndexingTest {
  private static final Random random = new Random();

  private static final Path collectionPath = new Path("data/bible+shakes.nopunc.gz");
  private static final String tmpPrefix = "tmp-"
      + InvertedIndexingTest.class.getCanonicalName() + "-" + random.nextInt(10000);

  @Test
  public void testInvertedIndexing() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.example.ir.BuildInvertedIndex.class.getCanonicalName(),
        IntegrationUtils.LOCAL_ARGS, "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-input", collectionPath.toString(),
        "-output", tmpPrefix,
        "-numReducers", "1"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    MapFile.Reader reader = new MapFile.Reader(new Path(tmpPrefix + "/part-r-00000"), conf);

    Text key = new Text();
    PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>> value =
        new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>();

    key.set("gold");

    reader.get(key, value);

    assertEquals(584, value.getLeftElement().get());
    ArrayListWritable<PairOfInts> postings = value.getRightElement();

    assertEquals(584, value.getLeftElement().get());

    assertEquals(5303, postings.get(0).getLeftElement());
    assertEquals(684030, postings.get(100).getLeftElement());
    assertEquals(1634312, postings.get(200).getLeftElement());

    reader.close();
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(InvertedIndexingTest.class);
  }
}
