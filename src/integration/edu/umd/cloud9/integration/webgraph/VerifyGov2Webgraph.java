package edu.umd.cloud9.integration.webgraph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Random;
import junit.framework.JUnit4TestAdapter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import edu.umd.cloud9.integration.IntegrationUtils;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.map.HMapSFW;
import edu.umd.cloud9.webgraph.data.AnchorText;
import edu.umd.cloud9.webgraph.driver.TrecDriver;

public class VerifyGov2Webgraph {
  private static final Random rand = new Random();
  private static final String tmp = "/tmp-" + VerifyGov2Webgraph.class.getSimpleName() + rand.nextInt(10000);

  private static final String collectionPath =
    "/shared/collections/gov2/collection.raw/gov2-corpus/GX000";
  private static final String docnoMapping =
    "/shared/indexes/gov2/docno-mapping.dat";
  private static final String collectionOutput = tmp + "/webgraph-gov2";

  // Galago: part 00000, key = 400
  private ImmutableMap<String, Float> anchorList1 = ImmutableMap.of(
    "mine safety health administration", 5.5f,
    "mine safety health administration msha", 1.25f,
    "msha", 1.25f,
    "safety health mining", 0.25f);

  // Galago: part 00000, key = 400
  private ImmutableMap<String, int[]> anchorSources1 = ImmutableMap.of(
    "mine safety health administration",
    new int[] {28502, 11970, 11445, 65562, 67427, 6338},
    "mine safety health administration msha",
    new int[] {25765, 24550, 14962, 82536, 68902, 46419, 35554, 6461, 17709},
    "msha",
    new int[] {25765, 1050, 35317},
    "safety health mining",
    new int[] {29107});

  // Galago: part 00010, key = 210
  private ImmutableMap<String, Float> anchorList2 = ImmutableMap.of(
    "hanford", 3.5f,
    "richland operations office rl", 0.5f);

  // Galago: part 00010, key = 210
  private ImmutableMap<String, int[]> anchorSources2 = ImmutableMap.of(
    "hanford",
    new int[] {55133, 89334, 51706, 52487, 44864, 39214},
    "richland operations office rl",
    new int[] {51706});

  @Test
  public void runTrecDriver() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(collectionPath)));

    fs.delete(new Path(collectionOutput), true);

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "commons-lang"));
    jars.add(IntegrationUtils.getJar("lib", "tools"));
    jars.add(IntegrationUtils.getJar("lib", "htmlparser"));
    jars.add(IntegrationUtils.getJar("lib", "pcj"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    TrecDriver.main(new String[] {libjars,
        IntegrationUtils.D_JT, IntegrationUtils.D_NN,
        "-input", collectionPath, "-output", collectionOutput,
        "-collection", "gov2", "-docno", docnoMapping,
        "-caw", "-normalizer", "edu.umd.cloud9.webgraph.normalizer.AnchorTextBasicNormalizer"});
  }

  @Test
  public void verifyAnchors() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    ArrayListWritable<AnchorText> value = new ArrayListWritable<AnchorText>();

    reader = new SequenceFile.Reader(fs,
        new Path(collectionOutput + "/weightedReverseWebGraph/part-00000"), fs.getConf());
    reader.next(key, value);
    reader.next(key, value);
    verifyWeights(anchorList1, value);
    verifySources(anchorSources1, value);

    reader = new SequenceFile.Reader(fs,
        new Path(collectionOutput + "/weightedReverseWebGraph/part-00010"), fs.getConf());
    reader.next(key, value);
    reader.next(key, value);
    verifyWeights(anchorList2, value);
    verifySources(anchorSources2, value);
  }

  private void verifyWeights(Map<String, Float> anchor, ArrayListWritable<AnchorText> value) {
    for (int i = 0; i < value.size(); i++) {
      if(anchor.containsKey(value.get(i).getText())) {
        assertEquals(anchor.get(value.get(i).getText()), value.get(i).getWeight(), 10e-6);
      }
    }
  }

  private void verifySources(Map<String, int[]> anchor, ArrayListWritable<AnchorText> value) {
    for (int i = 0; i < value.size(); i++) {
      if(anchor.containsKey(value.get(i).getText())) {
        int[] srcs = value.get(i).getDocuments();
        assertEquals(anchor.get(value.get(i).getText()).length, srcs.length);
        for(int j = 0; j < srcs.length; j++) {
          assertEquals(anchor.get(value.get(i).getText())[j], srcs[j]);
        }
      }
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyGov2Webgraph.class);
  }
}
