package edu.umd.cloud9.integration.example.pagerank;

import static org.junit.Assert.assertTrue;

import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import tl.lin.data.pair.PairOfStrings;

import com.google.common.base.Joiner;

import edu.umd.cloud9.integration.IntegrationUtils;

public class SimplePageRankIT {
  private static final Random random = new Random();

  private static final Path collectionPath = new Path("sample-large.txt");
  private static final String tmpPrefix = "tmp-"
      + SimplePageRankIT.class.getCanonicalName() + "-" + random.nextInt(10000);

  @Test
  public void testPageRank() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    IntegrationUtils.exec("hadoop fs -put docs/exercises/sample-large.txt");
    assertTrue(fs.exists(collectionPath));

    String[] args;

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.BuildPageRankRecords.class.getCanonicalName(),
        "-input", "sample-large.txt",
        "-output", tmpPrefix + "-sample-large-PageRankRecords",
        "-numNodes", "1458"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    IntegrationUtils.exec("hadoop fs -mkdir " + tmpPrefix + "-sample-large-PageRank");

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.PartitionGraph.class.getCanonicalName(),
        "-input", tmpPrefix + "-sample-large-PageRankRecords",
        "-output", tmpPrefix + "-sample-large-PageRank/iter0000",
        "-numPartitions", "5",
        "-numNodes", "1458"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.RunPageRankBasic.class.getCanonicalName(),
        "-base", tmpPrefix + "-sample-large-PageRank",
        "-numNodes", "1458",
        "-start", "0",
        "-end", "10", "-useCombiner"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.FindMaxPageRankNodes.class.getCanonicalName(),
        "-input", tmpPrefix + "-sample-large-PageRank/iter0010",
        "-output", tmpPrefix + "-sample-large-PageRank-top10",
        "-top", "10"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    PairOfStrings pair = IntegrationUtils.exec("hadoop fs -cat " + tmpPrefix +
        "-sample-large-PageRank-top10/part-r-00000");

    assertTrue(pair.getLeftElement().contains("9369084\t-4.38753"));
    assertTrue(pair.getLeftElement().contains("8669492\t-4.45486"));
    assertTrue(pair.getLeftElement().contains("12486146\t-4.77488"));
    assertTrue(pair.getLeftElement().contains("9265639\t-4.855565"));
    assertTrue(pair.getLeftElement().contains("10912914\t-4.86802"));

    IntegrationUtils.exec("hadoop fs -rm sample-large.txt");
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(SimplePageRankIT.class);
  }
}
