package edu.umd.cloud9.integration.collection.wikipedia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMapping;
import edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMappingBuilder;
import edu.umd.cloud9.integration.IntegrationUtils;

public class WikipediaBasicIntegrationTest {
  private static final Random random = new Random();
  private static final String tmpPrefix = "tmp-" + WikipediaBasicIntegrationTest.class.getCanonicalName() + "-" +
      random.nextInt(10000);

  public void testWikiDocnoMapping(String language, String input, String docid1, String docid2,
      int numDisamb, int numArticles, int total) throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);
    
    assertTrue(fs.exists(new Path(input)));

    String mappingFile = tmpPrefix + "-" + language + "wiki-mapping.dat";

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "bliki-core"));
    jars.add(IntegrationUtils.getJar("lib", "guava"));
    jars.add(IntegrationUtils.getJar("lib", "dsiutils"));
    jars.add(IntegrationUtils.getJar("lib", "fastutil"));
    jars.add(IntegrationUtils.getJar("lib", "sux4j"));
    jars.add(IntegrationUtils.getJar("lib", "commons-collections"));
    jars.add(IntegrationUtils.getJar("lib", "commons-lang"));
    jars.add(IntegrationUtils.getJar("lib", "tools"));
    jars.add(IntegrationUtils.getJar("lib", "maxent"));
    jars.add(IntegrationUtils.getJar("dist", "cloud9"));

    String libjars = String.format("-libjars=%s", Joiner.on(",").join(jars));

    String[] args = new String[] { "hadoop jar", IntegrationUtils.getJar("dist", "cloud9"),
        edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMappingBuilder.class.getCanonicalName(),
        libjars,
        "-" + WikipediaDocnoMappingBuilder.INPUT_OPTION + "=" + input,
        "-" + WikipediaDocnoMappingBuilder.OUTPUT_FILE_OPTION + "=" + mappingFile,
        "-" + WikipediaDocnoMappingBuilder.LANGUAGE_OPTION + "=" + language
    };

    List<Integer> counts = IntegrationUtils.execWiki(Joiner.on(" ").join(args));

    WikipediaDocnoMapping mapping = new WikipediaDocnoMapping();
    mapping.loadMapping(new Path(mappingFile), fs);

    System.out.println("DISAMBIGUATION = " + numDisamb + "; ARTICLE = " + numArticles + "; TOTAL = " + total);
    System.out.println("DOCNO 0 = " + mapping.getDocid(1));
    System.out.println("DOCNO 100000 = " + mapping.getDocid(100000));

    // docno to docid
    assertEquals(docid1, mapping.getDocid(1));
    assertEquals(docid2, mapping.getDocid(100000));

    // docid to docno
    assertEquals(1, mapping.getDocno(docid1));
    assertEquals(100000, mapping.getDocno(docid2));
    
    // # of disamb pages
    assertEquals(numDisamb, (int) counts.get(0));
    // # of articles
    assertEquals(numArticles, (int) counts.get(1));
    // total #
    assertEquals(total, (int) counts.get(2));
  }

  @Test
  public void testAllWikis() throws Exception {
    testWikiDocnoMapping("en", "/shared/collections/wikipedia/raw/enwiki-20121201-pages-articles", "12", "189362",
        123666, 4033137, 12961996);
    testWikiDocnoMapping("cs", "/shared/collections/wikipedia/raw/cswiki-20121215-pages-articles.xml", "4", "344433",
        7800, 248999, 497398);
    testWikiDocnoMapping("de", "/shared/collections/wikipedia/raw/dewiki-20121215-pages-articles.xml", "1", "297141",
        174678, 1326111, 3001626);
    testWikiDocnoMapping("es", "/shared/collections/wikipedia/raw/eswiki-20121130-pages-articles.xml", "7", "358642",
        36669, 1092193, 2611748);
    testWikiDocnoMapping("ar", "/shared/collections/wikipedia/raw/arwiki-20121218-pages-articles.xml", "7", "572997",
        3789, 237860, 529641);
    testWikiDocnoMapping("zh", "/shared/collections/wikipedia/raw/zhwiki-20121210-pages-articles.xml", "13", "456258",
        17992, 602267, 2067973);
    testWikiDocnoMapping("tr", "/shared/collections/wikipedia/raw/trwiki-20121217-pages-articles.xml", "5", "432151",
        5938, 240952, 589118);
  }
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(WikipediaBasicIntegrationTest.class);
  }
}
