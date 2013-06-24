package edu.umd.cloud9.collection.wikipedia;

import static org.junit.Assert.assertTrue;

import java.io.File;

import junit.framework.JUnit4TestAdapter;

import org.apache.commons.io.FileUtils;
import org.junit.Test;

import edu.umd.cloud9.collection.wikipedia.language.EnglishWikipediaPage;

public class WikipediaParsingTest {

  @Test
  public void testId12() throws Exception {
    String raw = FileUtils.readFileToString(
        new File("src/test/edu/umd/cloud9/collection/wikipedia/enwiki-20120104-id12.txt"));
    WikipediaPage page = new EnglishWikipediaPage();
    WikipediaPage.readPage(page, raw);

    String content = page.getContent();
    assertTrue(content.contains("\nAnarchism is generally defined as the political philosophy which holds the state to be undesirable, unnecessary, and harmful, or alternatively as opposing authority and hierarchical organization in the conduct of human relations. Proponents of anarchism, known as \"anarchists\", advocate stateless societies based on non-hierarchical voluntary associations.\n"));
  }

  @Test
  public void testId39() throws Exception {
    String raw = FileUtils.readFileToString(
        new File("src/test/edu/umd/cloud9/collection/wikipedia/enwiki-20120104-id39.txt"));
    WikipediaPage page = new EnglishWikipediaPage();
    WikipediaPage.readPage(page, raw);

    String content = page.getContent();
    assertTrue(content.contains("\nAlbedo, or reflection coefficient, is the diffuse reflectivity or reflecting power of a surface. "));
  }

  @Test
  public void testId290() throws Exception {
    String raw = FileUtils.readFileToString(
        new File("src/test/edu/umd/cloud9/collection/wikipedia/enwiki-20120104-id290.txt"));
    WikipediaPage page = new EnglishWikipediaPage();
    WikipediaPage.readPage(page, raw);

    String content = page.getContent();
    System.out.println(page.getContent());

    assertTrue(content.contains("\nA (named a, plural aes) is the first letter and a vowel in the basic modern Latin alphabet. "));
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(WikipediaParsingTest.class);
  }
}
