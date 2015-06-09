package edu.umd.cloud9.integration.example.bfs;

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

public class BfsIT {
  private static final Random random = new Random();

  private static final Path collectionPath =
      new Path("/collections/wikipedia/enwiki-20121201-pages-articles");
  private static final String tmpPrefix = "tmp-"
      + BfsIT.class.getCanonicalName() + "-" + random.nextInt(10000);

  @Test
  public void testBfs() throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);

    assertTrue(fs.exists(collectionPath));

    String[] args;

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMappingBuilder.class.getCanonicalName(),
        "-input", collectionPath.toString(),
        "-output_file", tmpPrefix + "-enwiki-20121201-docno.dat",
        "-wiki_language", "en", "-keep_all"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.collection.wikipedia.RepackWikipedia.class.getCanonicalName(),
        "-input", collectionPath.toString(),
        "-output", tmpPrefix + "-enwiki-20121201.block",
        "-mapping_file", tmpPrefix + "-enwiki-20121201-docno.dat",
        "-wiki_language", "en",
        "-compression_type", "block"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.collection.wikipedia.graph.ExtractWikipediaLinkGraph.class.getCanonicalName(),
        "-input", tmpPrefix + "-enwiki-20121201.block",
        "-edges_output", tmpPrefix + "-enwiki-20121201.edges",
        "-adjacency_list_output", tmpPrefix + "-enwiki-20121201.adj",
        "-num_partitions", "10"};

    PairOfStrings out = IntegrationUtils.exec(Joiner.on(" ").join(args));
    String errorOut = out.getRightElement();

    assertTrue(errorOut.contains("EDGES=121762273"));
    assertTrue(errorOut.contains("TOTAL_VERTICES=12961996"));
    assertTrue(errorOut.contains("VERTICES_WITH_OUTLINKS=10813673"));

    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.bfs.EncodeBfsGraph.class.getCanonicalName(),
        "-input", tmpPrefix + "-enwiki-20121201.adj",
        "-output", tmpPrefix + "-enwiki-20121201.bfs/iter0000",
        "-src", "12"};

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    // First iteration of BFS.
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.bfs.IterateBfs.class.getCanonicalName(),
        "-input", tmpPrefix + "-enwiki-20121201.bfs/iter0000",
        "-output", tmpPrefix + "-enwiki-20121201.bfs/iter0001",
        "-num_partitions", "10"};

    out = IntegrationUtils.exec(Joiner.on(" ").join(args));
    errorOut = out.getRightElement();

    assertTrue(errorOut.contains("ReachableInMapper=1"));
    assertTrue(errorOut.contains("ReachableInReducer=573"));

    // Second iteration of BFS.
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.bfs.IterateBfs.class.getCanonicalName(),
        "-input", tmpPrefix + "-enwiki-20121201.bfs/iter0001",
        "-output", tmpPrefix + "-enwiki-20121201.bfs/iter0002",
        "-num_partitions", "10"};

    out = IntegrationUtils.exec(Joiner.on(" ").join(args));
    errorOut = out.getRightElement();

    assertTrue(errorOut.contains("ReachableInMapper=573"));
    assertTrue(errorOut.contains("ReachableInReducer=37733"));
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(BfsIT.class);
  }
}
