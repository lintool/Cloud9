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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import edu.umd.cloud9.integration.IntegrationUtils;
import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.webgraph.DriverUtil;
import edu.umd.cloud9.webgraph.data.AnchorText;


public class VerifyGov2Webgraph {
  private static final Random rand = new Random();
  private static final String tmp = "/tmp/tmp-" + VerifyGov2Webgraph.class.getSimpleName() + rand.nextInt(10000);

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
  private ImmutableMap<String, ImmutableSet<Integer>> anchorSources1 = ImmutableMap.of(
    "mine safety health administration",
    ImmutableSet.of(28502, 11970, 11445, 65562, 67427, 6338),
    "mine safety health administration msha",
    ImmutableSet.of(25765, 24550, 14962, 82536, 68902, 46419, 35554, 6461, 17709),
    "msha",
    ImmutableSet.of(25765, 1050, 35317),
    "safety health mining",
    ImmutableSet.of(29107));

  // Galago: part 00010, key = 210
  private ImmutableMap<String, Float> anchorList2 = ImmutableMap.of(
    "hanford", 3.5f,
    "richland operations office rl", 0.5f);

  // Galago: part 00010, key = 210
  private ImmutableMap<String, ImmutableSet<Integer>> anchorSources2 = ImmutableMap.of(
    "hanford",
    ImmutableSet.of(55133, 89334, 51706, 52487, 44864, 39214),
    "richland operations office rl",
    ImmutableSet.of(51706));

  @Test
  public void runTests() throws Exception {
    runTrecDriver();
    verifyAnchors();
  }

  private void runTrecDriver() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(new Path(collectionPath)));

    fs.delete(new Path(collectionOutput), true);

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));
    jars.add(IntegrationUtils.getJar("lib", "guava"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "commons-lang"));
    jars.add(IntegrationUtils.getJar("lib", "tools"));
    jars.add(IntegrationUtils.getJar("lib", "htmlparser"));
    jars.add(IntegrationUtils.getJar("lib", "pcj"));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.webgraph.driver.TrecDriver.class.getCanonicalName(),
        String.format("-libjars=%s", Joiner.on(",").join(jars)),
        "-input", collectionPath,
        "-output", collectionOutput,
        "-collection", "gov2",
        "-docno", docnoMapping,
        "-caw",
        "-normalizer", edu.umd.cloud9.webgraph.normalizer.AnchorTextBasicNormalizer.class.getCanonicalName()};

    IntegrationUtils.exec(Joiner.on(" ").join(args));
  }

  private void verifyAnchors() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    SequenceFile.Reader reader;
    IntWritable key = new IntWritable();
    ArrayListWritable<AnchorText> value = new ArrayListWritable<AnchorText>();

    reader = new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.file(
        new Path(collectionOutput + "/" + DriverUtil.OUTPUT_WEGIHTED_REVERSE_WEBGRAPH + "/part-00000")));
    reader.next(key, value);
    reader.next(key, value);
    verifyWeights(anchorList1, value);
    verifySources(anchorSources1, value);
    reader.close();

    reader = new SequenceFile.Reader(fs.getConf(), SequenceFile.Reader.file(
        new Path(collectionOutput + "/" + DriverUtil.OUTPUT_WEGIHTED_REVERSE_WEBGRAPH + "/part-00010")));
    reader.next(key, value);
    reader.next(key, value);
    verifyWeights(anchorList2, value);
    verifySources(anchorSources2, value);
    reader.close();
  }

  private void verifyWeights(Map<String, Float> anchor, ArrayListWritable<AnchorText> value) {
    for (int i = 0; i < value.size(); i++) {
      if(anchor.containsKey(value.get(i).getText())) {
        assertEquals(anchor.get(value.get(i).getText()), value.get(i).getWeight(), 10e-6);
      }
    }
  }

  private void verifySources(Map<String, ImmutableSet<Integer>> anchor, ArrayListWritable<AnchorText> value) {
    for (int i = 0; i < value.size(); i++) {
      if(anchor.containsKey(value.get(i).getText())) {
        int[] srcs = value.get(i).getDocuments();
        assertEquals(anchor.get(value.get(i).getText()).size(), srcs.length);
        for(int j = 0; j < srcs.length; j++) {
          assertTrue(anchor.get(value.get(i).getText()).contains(srcs[j]));
        }
      }
    }
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(VerifyGov2Webgraph.class);
  }
}
