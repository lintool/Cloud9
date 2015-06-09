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

public class ClueWebPageRankIT {
  private static final Random random = new Random();

  private static final Path collectionPath =
      new Path("/collections/ClueWeb09/clueweb09en01-webgraph-adjacency.txt");
  private static final String tmpPrefix = "tmp-"
      + SimplePageRankIT.class.getCanonicalName() + "-" + random.nextInt(10000);

  @Test
  public void testPageRank() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    String[] args;
    PairOfStrings pair;

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.BuildPageRankRecords.class.getCanonicalName(),
        "-input", collectionPath.toString(),
        "-output", tmpPrefix + "-clueweb09en01-PageRankRecords",
        "-numNodes", "50220423"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    // Hash partitioning, basic
    IntegrationUtils.exec("hadoop fs -mkdir " + tmpPrefix + "-clueweb09en01-PageRank.hash.basic");

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.PartitionGraph.class.getCanonicalName(),
        "-input", tmpPrefix + "-clueweb09en01-PageRankRecords",
        "-output", tmpPrefix + "-clueweb09en01-PageRank.hash.basic/iter0000",
        "-numPartitions", "200",
        "-numNodes", "50220423"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.RunPageRankBasic.class.getCanonicalName(),
        "-base", tmpPrefix + "-clueweb09en01-PageRank.hash.basic",
        "-numNodes", "50220423",
        "-start", "0",
        "-end", "10",
        "-useInMapperCombiner"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.FindMaxPageRankNodes.class.getCanonicalName(),
        "-input", tmpPrefix + "-clueweb09en01-PageRank.hash.basic/iter0010",
        "-output", tmpPrefix + "-clueweb09en01-PageRank.hash.basic-top10",
        "-top", "10"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    pair = IntegrationUtils.exec("hadoop fs -cat " + tmpPrefix +
        "-clueweb09en01-PageRank.hash.basic-top10/part-r-00000");

    assertTrue(pair.getLeftElement().contains("16073008\t-6.381"));
    assertTrue(pair.getLeftElement().contains("42722712\t-6.425"));
    assertTrue(pair.getLeftElement().contains("16073696\t-6.552"));
    assertTrue(pair.getLeftElement().contains("16073003\t-6.604"));
    assertTrue(pair.getLeftElement().contains("47345600\t-6.610"));

    // Hash partitioning, Schimmy
    IntegrationUtils.exec("hadoop fs -mkdir " + tmpPrefix + "-clueweb09en01-PageRank.hash.schimmy");

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.PartitionGraph.class.getCanonicalName(),
        "-input", tmpPrefix + "-clueweb09en01-PageRankRecords",
        "-output", tmpPrefix + "-clueweb09en01-PageRank.hash.schimmy/iter0000",
        "-numPartitions", "200",
        "-numNodes", "50220423"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.RunPageRankSchimmy.class.getCanonicalName(),
        "-base", tmpPrefix + "-clueweb09en01-PageRank.hash.schimmy",
        "-numNodes", "50220423",
        "-start", "0",
        "-end", "10",
        "-useInMapperCombiner"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.FindMaxPageRankNodes.class.getCanonicalName(),
        "-input", tmpPrefix + "-clueweb09en01-PageRank.hash.schimmy/iter0010",
        "-output", tmpPrefix + "-clueweb09en01-PageRank.hash.schimmy-top10",
        "-top", "10"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    pair = IntegrationUtils.exec("hadoop fs -cat " + tmpPrefix +
        "-clueweb09en01-PageRank.hash.schimmy-top10/part-r-00000");

    assertTrue(pair.getLeftElement().contains("16073008\t-6.371"));
    assertTrue(pair.getLeftElement().contains("42722712\t-6.421"));
    assertTrue(pair.getLeftElement().contains("16073696\t-6.540"));
    assertTrue(pair.getLeftElement().contains("16073003\t-6.592"));
    assertTrue(pair.getLeftElement().contains("47345600\t-6.597"));

    // Range partitioning, basic
    IntegrationUtils.exec("hadoop fs -mkdir " + tmpPrefix + "-clueweb09en01-PageRank.range.basic");

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.PartitionGraph.class.getCanonicalName(),
        "-input", tmpPrefix + "-clueweb09en01-PageRankRecords",
        "-output", tmpPrefix + "-clueweb09en01-PageRank.range.basic/iter0000",
        "-numPartitions", "200",
        "-numNodes", "50220423",
        "-range"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.RunPageRankBasic.class.getCanonicalName(),
        "-base", tmpPrefix + "-clueweb09en01-PageRank.range.basic",
        "-numNodes", "50220423",
        "-start", "0",
        "-end", "10",
        "-useInMapperCombiner",
        "-range"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.FindMaxPageRankNodes.class.getCanonicalName(),
        "-input", tmpPrefix + "-clueweb09en01-PageRank.range.basic/iter0010",
        "-output", tmpPrefix + "-clueweb09en01-PageRank.range.basic-top10",
        "-top", "10"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    pair = IntegrationUtils.exec("hadoop fs -cat " + tmpPrefix +
        "-clueweb09en01-PageRank.range.basic-top10/part-r-00000");

    assertTrue(pair.getLeftElement().contains("16073008\t-6.381"));
    assertTrue(pair.getLeftElement().contains("42722712\t-6.425"));
    assertTrue(pair.getLeftElement().contains("16073696\t-6.552"));
    assertTrue(pair.getLeftElement().contains("16073003\t-6.604"));
    assertTrue(pair.getLeftElement().contains("47345600\t-6.610"));

    // Range partitioning, Schimmy
    IntegrationUtils.exec("hadoop fs -mkdir " + tmpPrefix + "-clueweb09en01-PageRank.range.schimmy");

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.PartitionGraph.class.getCanonicalName(),
        "-input", tmpPrefix + "-clueweb09en01-PageRankRecords",
        "-output", tmpPrefix + "-clueweb09en01-PageRank.range.schimmy/iter0000",
        "-numPartitions", "200",
        "-numNodes", "50220423",
        "-range"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.RunPageRankSchimmy.class.getCanonicalName(),
        "-base", tmpPrefix + "-clueweb09en01-PageRank.range.schimmy",
        "-numNodes", "50220423",
        "-start", "0",
        "-end", "10",
        "-useInMapperCombiner",
        "-range"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.pagerank.FindMaxPageRankNodes.class.getCanonicalName(),
        "-input", tmpPrefix + "-clueweb09en01-PageRank.range.schimmy/iter0010",
        "-output", tmpPrefix + "-clueweb09en01-PageRank.range.schimmy-top10",
        "-top", "10"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    pair = IntegrationUtils.exec("hadoop fs -cat " + tmpPrefix +
        "-clueweb09en01-PageRank.range.schimmy-top10/part-r-00000");

    assertTrue(pair.getLeftElement().contains("16073008\t-6.372"));
    assertTrue(pair.getLeftElement().contains("42722712\t-6.420"));
    assertTrue(pair.getLeftElement().contains("16073696\t-6.541"));
    assertTrue(pair.getLeftElement().contains("16073003\t-6.593"));
    assertTrue(pair.getLeftElement().contains("47345600\t-6.599"));
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(ClueWebPageRankIT.class);
  }
}
