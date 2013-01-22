package edu.umd.cloud9.integration.collection.wikipedia;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;

import junit.framework.JUnit4TestAdapter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import edu.umd.cloud9.collection.DocnoMapping;
import edu.umd.cloud9.collection.trecweb.Gov2DocnoMapping;
import edu.umd.cloud9.collection.trecweb.TrecWebDocumentInputFormat;
import edu.umd.cloud9.collection.trecweb.Wt10gDocnoMapping;
import edu.umd.cloud9.collection.wikipedia.BuildWikipediaDocnoMapping;
import edu.umd.cloud9.collection.wikipedia.WikipediaDocnoMapping;
import edu.umd.cloud9.integration.IntegrationUtils;

public class IntegrationTest {
  private static final Random random = new Random();


  private static final String tmpPrefix = "tmp-" + IntegrationTest.class.getCanonicalName() + "-" + random.nextInt(10000);

  public void testWikiDocnoMapping(String language, String input, String docid1, String docid2, int numDisamb) throws Exception {
    Configuration conf = IntegrationUtils.getBespinConfiguration();
    FileSystem fs = FileSystem.get(conf);
    
    assertTrue(fs.exists(new Path(input)));

    String mappingFile = tmpPrefix + "-" + language + "wiki-mapping.dat";

    List<String> jars = Lists.newArrayList();
    jars.add(IntegrationUtils.getJar("lib", "bliki-core"));
    jars.add(IntegrationUtils.getJar("lib", "guava-13"));
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
        edu.umd.cloud9.collection.wikipedia.BuildWikipediaDocnoMapping.class.getCanonicalName(),
        libjars,
        "-" + BuildWikipediaDocnoMapping.INPUT_OPTION + "=" + input,
        "-" + BuildWikipediaDocnoMapping.OUTPUT_PATH_OPTION + "=" + mappingFile + ".tmp",
        "-" + BuildWikipediaDocnoMapping.OUTPUT_FILE_OPTION + "=" + mappingFile,
        "-" + BuildWikipediaDocnoMapping.LANG_OPTION + "=" + language
    };

    int numDisambiguationPages = IntegrationUtils.execWiki(Joiner.on(" ").join(args));

    WikipediaDocnoMapping mapping = new WikipediaDocnoMapping();
    mapping.loadMapping(new Path(mappingFile), fs);

    // docno to docid
    assertEquals(docid1, mapping.getDocid(1));
    assertEquals(docid2, mapping.getDocid(100000));

    // docid to docno
    assertEquals(1, mapping.getDocno(docid1));
    assertEquals(100000, mapping.getDocno(docid2));
    
    // # of disamb pages
    assertEquals(numDisamb, numDisambiguationPages);
  }

  @Test
  public void testAllWikis() {
    try {
      testWikiDocnoMapping("en", "/shared/collections/wikipedia/raw/enwiki-20121201-pages-articles.xml", "12", "189315", 123666);
      testWikiDocnoMapping("cs", "/shared/collections/wikipedia/raw/cswiki-20121215-pages-articles.xml", "4", "277861", 7800);
      testWikiDocnoMapping("de", "/shared/collections/wikipedia/raw/dewiki-20121215-pages-articles.xml", "1", "282456", 174684);
      testWikiDocnoMapping("es", "/shared/collections/wikipedia/raw/eswiki-20121130-pages-articles.xml", "7", "316173", 36669);
      testWikiDocnoMapping("ar", "/shared/collections/wikipedia/raw/arwiki-20121218-pages-articles.xml", "7", "333981", 3789);
      testWikiDocnoMapping("zh", "/shared/collections/wikipedia/raw/zhwiki-20121210-pages-articles.xml", "13", "454230", 17992);
      testWikiDocnoMapping("tr", "/shared/collections/wikipedia/raw/trwiki-20121217-pages-articles.xml", "5", "318972", 5938);
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Error when running test case!");
    }
  }
  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(IntegrationTest.class);
  }
}
