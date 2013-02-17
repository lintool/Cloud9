package edu.umd.cloud9.integration.example.bigram;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.junit.Test;

import com.google.common.base.Joiner;

import edu.umd.cloud9.example.bigram.BigramRelativeFrequencyJson;
import edu.umd.cloud9.integration.IntegrationUtils;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class BigramRelativeFrequencyTest {
  private static final TupleFactory TUPLE_FACTORY = TupleFactory.getInstance();

  private static final Random random = new Random();

  private static final Path collectionPath = new Path("data/bible+shakes.nopunc.gz");
  private static final String tmpPrefix = "tmp-"
      + BigramRelativeFrequencyTest.class.getCanonicalName() + "-" + random.nextInt(10000);

  @Test
  public void testBigramRelativeFrequencyBase() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    assertTrue(fs.exists(collectionPath));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.example.bigram.BigramRelativeFrequency.class.getCanonicalName(),
        IntegrationUtils.LOCAL_ARGS, "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-input", collectionPath.toString(),
        "-output", tmpPrefix + "-base",
        "-numReducers", "1"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    SequenceFile.Reader reader = new SequenceFile.Reader(conf,
        SequenceFile.Reader.file(new Path(tmpPrefix + "-base/part-r-00000")));

    PairOfStrings pair = new PairOfStrings();
    FloatWritable f = new FloatWritable();

    reader.next(pair, f);
    assertEquals("&c", pair.getLeftElement());
    assertEquals("*", pair.getRightElement());
    assertEquals(17f, f.get(), 10e-6);

    for (int i = 0; i < 100; i++) {
      reader.next(pair, f);
    }

    assertEquals("'dear", pair.getLeftElement());
    assertEquals("*", pair.getRightElement());
    assertEquals(2f, f.get(), 10e-6);

    reader.next(pair, f);
    assertEquals("'dear", pair.getLeftElement());
    assertEquals("lord", pair.getRightElement());
    assertEquals(1f, f.get(), 10e-6);

    reader.close();
  }

  @Test
  public void testBigramRelativeFrequencyJson() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    assertTrue(fs.exists(collectionPath));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.example.bigram.BigramRelativeFrequencyJson.class.getCanonicalName(),
        IntegrationUtils.LOCAL_ARGS, "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-input", collectionPath.toString(),
        "-output", tmpPrefix + "-json",
        "-numReducers", "1"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    SequenceFile.Reader reader = new SequenceFile.Reader(conf,
        SequenceFile.Reader.file(new Path(tmpPrefix + "-json/part-r-00000")));

    BigramRelativeFrequencyJson.MyTuple json = new BigramRelativeFrequencyJson.MyTuple();
    FloatWritable f = new FloatWritable();

    reader.next(json, f);
    assertEquals("&c", json.getJsonObject().get("Left").getAsString());
    assertEquals("*", json.getJsonObject().get("Right").getAsString());
    assertEquals(17f, f.get(), 10e-6);

    for (int i = 0; i < 100; i++) {
      reader.next(json, f);
    }

    assertEquals("'dear", json.getJsonObject().get("Left").getAsString());
    assertEquals("*", json.getJsonObject().get("Right").getAsString());
    assertEquals(2f, f.get(), 10e-6);

    reader.next(json, f);
    assertEquals("'dear", json.getJsonObject().get("Left").getAsString());
    assertEquals("lord", json.getJsonObject().get("Right").getAsString());
    assertEquals(1f, f.get(), 10e-6);

    reader.close();
  }

  @Test
  public void testBigramRelativeFrequencyTuple() throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.getLocal(conf);

    assertTrue(fs.exists(collectionPath));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.example.bigram.BigramRelativeFrequencyTuple.class.getCanonicalName(),
        IntegrationUtils.LOCAL_ARGS, "-libjars=" + IntegrationUtils.getJar("lib", "guava"),
        "-input", collectionPath.toString(),
        "-output", tmpPrefix + "-tuple",
        "-numReducers", "1"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    SequenceFile.Reader reader = new SequenceFile.Reader(conf,
        SequenceFile.Reader.file(new Path(tmpPrefix + "-tuple/part-r-00000")));

    Tuple tuple = TUPLE_FACTORY.newTuple();
    FloatWritable f = new FloatWritable();

    reader.next(tuple, f);
    assertEquals("&c", tuple.get(0).toString());
    assertEquals("*", tuple.get(1).toString());
    assertEquals(17f, f.get(), 10e-6);

    for (int i = 0; i < 100; i++) {
      reader.next(tuple, f);
    }

    assertEquals("'dear", tuple.get(0).toString());
    assertEquals("*", tuple.get(1).toString());
    assertEquals(2f, f.get(), 10e-6);

    reader.next(tuple, f);
    assertEquals("'dear", tuple.get(0).toString());
    assertEquals("lord", tuple.get(1).toString());
    assertEquals(1f, f.get(), 10e-6);

    reader.close();
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(BigramRelativeFrequencyTest.class);
  }
}
