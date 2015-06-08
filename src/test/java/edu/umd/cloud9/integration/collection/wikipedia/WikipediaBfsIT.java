package edu.umd.cloud9.integration.collection.wikipedia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMappingBuilder;
import edu.umd.cloud9.integration.IntegrationUtils;

public class WikipediaBfsIT {
  private static final Random random = new Random();
  private static final String tmpPrefix = 
      "tmp-" + WikipediaBfsIT.class.getCanonicalName() + "-" + random.nextInt(10000);

  @Test
  public void tesBfs() throws Exception {
    String input = "/collections/wikipedia/raw/enwiki-20121201-pages-articles";
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);
    
    assertTrue(fs.exists(new Path(input)));

    String mappingFile = tmpPrefix + "-enwiki-mapping.dat";

    Map<String, Integer> values;

    // Build the mapping.
    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMappingBuilder.class.getCanonicalName(),
        "-" + WikipediaDocnoMappingBuilder.INPUT_OPTION + "=" + input,
        "-" + WikipediaDocnoMappingBuilder.OUTPUT_FILE_OPTION + "=" + mappingFile,
        "-keep_all"
    };

    values = IntegrationUtils.execKeyValueExtractor(Joiner.on(" ").join(args),
        ImmutableSet.of("TOTAL"));

    assertEquals(12961996, (int) values.get("TOTAL"));

    // Repack the wiki.
    String repackedWiki = tmpPrefix + "-enwiki.block";
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.collection.wikipedia.RepackWikipedia.class.getCanonicalName(),
        "-input=" + input,
        "-mapping_file=" + mappingFile,
        "-output=" + repackedWiki,
        "-compression_type=block",
    };

    values = IntegrationUtils.execKeyValueExtractor(Joiner.on(" ").join(args),
        ImmutableSet.of("TOTAL"));

    assertEquals(12961996, (int) values.get("TOTAL"));

    // Extract the link graph.
    String wikiEdges = tmpPrefix + "-enwiki.edges";
    String wikiAdj = tmpPrefix + "-enwiki.adj";
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.collection.wikipedia.graph.ExtractWikipediaLinkGraph.class.getCanonicalName(),
        "-input=" + repackedWiki,
        "-edges_output=" + wikiEdges,
        "-adjacency_list_output=" + wikiAdj,
        "-num_partitions=10"
    };

    values = IntegrationUtils.execKeyValueExtractor(Joiner.on(" ").join(args),
        ImmutableSet.of("EDGES", "TOTAL_VERTICES", "VERTICES_WITH_OUTLINKS"));

    assertEquals(121762273, (int) values.get("EDGES"));
    assertEquals(12961996, (int) values.get("TOTAL_VERTICES"));
    assertEquals(10813673, (int) values.get("VERTICES_WITH_OUTLINKS"));

    // Build Bfs records.
    String bfsBase = tmpPrefix + "-enwiki.bfs";
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.bfs.EncodeBfsGraph.class.getCanonicalName(),
        "-input=" + wikiAdj,
        "-output=" + bfsBase + "/iter0000",
        "-src=12"
    };

    IntegrationUtils.exec(Joiner.on(" ").join(args));

    // Iteration 1.
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.bfs.IterateBfs.class.getCanonicalName(),
        "-input=" + bfsBase + "/iter0000",
        "-output=" + bfsBase + "/iter0001",
        "-num_partitions=10"
    };

    values = IntegrationUtils.execKeyValueExtractor(Joiner.on(" ").join(args),
        ImmutableSet.of("ReachableInReducer"));

    assertEquals(573, (int) values.get("ReachableInReducer"));

    // Iteration 2.
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.bfs.IterateBfs.class.getCanonicalName(),
        "-input=" + bfsBase + "/iter0001",
        "-output=" + bfsBase + "/iter0002",
        "-num_partitions=10"
    };

    values = IntegrationUtils.execKeyValueExtractor(Joiner.on(" ").join(args),
        ImmutableSet.of("ReachableInReducer"));

    assertEquals(37733, (int) values.get("ReachableInReducer"));

    // Iteration 3.
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.bfs.IterateBfs.class.getCanonicalName(),
        "-input=" + bfsBase + "/iter0002",
        "-output=" + bfsBase + "/iter0003",
        "-num_partitions=10"
    };

    values = IntegrationUtils.execKeyValueExtractor(Joiner.on(" ").join(args),
        ImmutableSet.of("ReachableInReducer"));

    assertEquals(845452, (int) values.get("ReachableInReducer"));

    // Iteration 4.
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.bfs.IterateBfs.class.getCanonicalName(),
        "-input=" + bfsBase + "/iter0003",
        "-output=" + bfsBase + "/iter0004",
        "-num_partitions=10"
    };

    values = IntegrationUtils.execKeyValueExtractor(Joiner.on(" ").join(args),
        ImmutableSet.of("ReachableInReducer"));

    assertEquals(3596247, (int) values.get("ReachableInReducer"));

    // Iteration 5.
    args = new String[] { "hadoop jar", IntegrationUtils.getJar("target", "cloud9"),
        edu.umd.cloud9.example.bfs.IterateBfs.class.getCanonicalName(),
        "-input=" + bfsBase + "/iter0004",
        "-output=" + bfsBase + "/iter0005",
        "-num_partitions=10"
    };

    values = IntegrationUtils.execKeyValueExtractor(Joiner.on(" ").join(args),
        ImmutableSet.of("ReachableInReducer"));

    assertEquals(5236564, (int) values.get("ReachableInReducer"));

  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(WikipediaBfsIT.class);
  }
}
