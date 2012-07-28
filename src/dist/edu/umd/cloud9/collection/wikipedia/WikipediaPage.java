/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.collection.wikipedia;

import info.bliki.wiki.filter.PlainTextConverter;
import info.bliki.wiki.model.WikiModel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.Indexable;

/**
 * A page from Wikipedia.
 * 
 * @author Jimmy Lin
 */
public class WikipediaPage extends Indexable {

  /**
   * Start delimiter of the page, which is &lt;<code>page</code>&gt;.
   */
  public static final String XML_START_TAG = "<page>";

  /**
   * End delimiter of the page, which is &lt;<code>/page</code>&gt;.
   */
  public static final String XML_END_TAG = "</page>";

  private String page;
  private String title;
  private String mId;
  private int textStart;
  private int textEnd;
  private boolean isRedirect;
  private boolean isDisambig;
  private boolean isStub;
  private String language;

  private WikiModel wikiModel;
  private PlainTextConverter textConverter;

  /**
   * Creates an empty <code>WikipediaPage</code> object.
   */
  public WikipediaPage() {
    wikiModel = new WikiModel("", "");
    textConverter = new PlainTextConverter();
  }

  /**
   * Deserializes this object.
   */
  public void write(DataOutput out) throws IOException {
    byte[] bytes = page.getBytes();
    WritableUtils.writeVInt(out, bytes.length);
    out.write(bytes, 0, bytes.length);
    out.writeUTF(language == null ? "unk" : language);
  }

  /**
   * Serializes this object.
   */
  public void readFields(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    byte[] bytes = new byte[length];
    in.readFully(bytes, 0, length);
    WikipediaPage.readPage(this, new String(bytes));
    language = in.readUTF();
  }

  /**
   * Returns the article title (i.e., the docid).
   */
  public String getDocid() {
    return mId;
  }

  public void setLanguage(String language) {
    this.language = language;
  }

  public String getLanguage() {
    return this.language;
  }


  // Explictly remove <ref>...</ref>, because there are screwy things like this:
  //   <ref>[http://www.interieur.org/<!-- Bot generated title -->]</ref>
  // where "http://www.interieur.org/<!--" gets interpreted as the URL by
  // Bliki in conversion to text
  private static final Pattern REF = Pattern.compile("<ref>.*?</ref>");

  private static final Pattern LANG_LINKS = Pattern.compile("\\[\\[[a-z\\-]+:[^\\]]+\\]\\]");
  private static final Pattern DOUBLE_CURLY = Pattern.compile("\\{\\{.*?\\}\\}");

  private static final Pattern URL = Pattern.compile("http://[^ <]+"); // Note, don't capture possible HTML tag

  private static final Pattern HTML_TAG = Pattern.compile("<[^!][^>]*>"); // Note, don't capture comments
  private static final Pattern HTML_COMMENT = Pattern.compile("<!--.*?-->", Pattern.DOTALL);

  /**
   * Returns the contents of this page (title + text).
   */
  public String getContent() {
    String s = getWikiMarkup();

    // Bliki doesn't seem to properly handle inter-language links, so remove manually.
    s = LANG_LINKS.matcher(s).replaceAll(" ");

    wikiModel.setUp();
    s = getTitle() + "\n" + wikiModel.render(textConverter, s);
    wikiModel.tearDown();

    // The way the some entities are encoded, we have to unescape twice.
    s = StringEscapeUtils.unescapeHtml(StringEscapeUtils.unescapeHtml(s));

    s = REF.matcher(s).replaceAll(" ");
    s = HTML_COMMENT.matcher(s).replaceAll(" ");

    // Sometimes, URL bumps up against comments e.g., <!-- http://foo.com/-->
    // Therefore, we want to remove the comment first; otherwise the URL pattern might eat up
    // the comment terminator.
    s = URL.matcher(s).replaceAll(" ");
    s = DOUBLE_CURLY.matcher(s).replaceAll(" ");
    s = HTML_TAG.matcher(s).replaceAll(" ");

    return s;
  }

  public String getDisplayContent() {
    wikiModel.setUp();
    String s = "<h1>" + getTitle() + "</h1>\n" + wikiModel.render(getWikiMarkup());
    wikiModel.tearDown();

    s = DOUBLE_CURLY.matcher(s).replaceAll(" ");

    return s;
  }

  @Override
  public String getDisplayContentType() {
    return "text/html";
  }

  /**
   * Returns the raw XML of this page.
   */
  public String getRawXML() {
    return page;
  }

  /**
   * Returns the text of this page.
   */
  public String getWikiMarkup() {
    if (textStart == -1)
      return null;

    return page.substring(textStart + 27, textEnd);
  }

  /**
   * Returns the title of this page.
   */
  public String getTitle() {
    return title;
  }

  /**
   * Checks to see if this page is a disambiguation page. A
   * <code>WikipediaPage</code> is either an article, a disambiguation page,
   * a redirect page, or an empty page.
   * 
   * @return <code>true</code> if this page is a disambiguation page
   */
  public boolean isDisambiguation() {
    return isDisambig;
  }

  /**
   * Checks to see if this page is a redirect page. A
   * <code>WikipediaPage</code> is either an article, a disambiguation page,
   * a redirect page, or an empty page.
   * 
   * @return <code>true</code> if this page is a redirect page
   */
  public boolean isRedirect() {
    return isRedirect;
  }

  /**
   * Checks to see if this page is an empty page. A <code>WikipediaPage</code>
   * is either an article, a disambiguation page, a redirect page, or an empty
   * page.
   * 
   * @return <code>true</code> if this page is an empty page
   */
  public boolean isEmpty() {
    return textStart == -1;
  }

  /**
   * Checks to see if this article is a stub. Return value is only meaningful
   * if this page isn't a disambiguation page, a redirect page, or an empty
   * page.
   * 
   * @return <code>true</code> if this article is a stub
   */
  public boolean isStub() {
    return isStub;
  }

  /**
   * Checks to see if this page is an actual article, and not, for example,
   * "File:", "Category:", "Wikipedia:", etc.
   *
   * @return <code>true</code> if this page is an actual article
   */
  public boolean isArticle() {
    return !(getTitle().startsWith("File:") || getTitle().startsWith("Category:")
        || getTitle().startsWith("Special:") || getTitle().startsWith("Wikipedia:")
        || getTitle().startsWith("Wikipedia:") || getTitle().startsWith("Template:")
        || getTitle().startsWith("Portal:"));
  }


  /**
   * Returns the inter-language link to a specific language (if any).
   * 
   * @param lang
   *            language
   * @return title of the article in the foreign language if link exists,
   *         <code>null</code> otherwise
   */
  public String findInterlanguageLink(String lang) {
    int start = page.indexOf("[[" + lang + ":");

    if (start < 0)
      return null;

    int end = page.indexOf("]]", start);

    if (end < 0)
      return null;

    // Some pages have malformed links. For example, "[[de:Frances Willard]"
    // in enwiki-20081008-pages-articles.xml.bz2 has only one closing square
    // bracket. Temporary solution is to ignore malformed links (instead of
    // trying to hack around them).
    String link = page.substring(start + 3 + lang.length(), end);

    // If a newline is found, it probably means that the link is malformed
    // (see above comment). Abort in this case.
    if (link.indexOf("\n") != -1) {
      return null;
    }

    if (link.length() == 0)
      return null;

    return link;
  }

  public List<String> extractLinkDestinations() {
    int start = 0;
    List<String> links = new ArrayList<String>();

    while (true) {
      start = page.indexOf("[[", start);

      if (start < 0)
        break;

      int end = page.indexOf("]]", start);

      if (end < 0)
        break;

      String text = page.substring(start + 2, end);

      // skip empty links
      if (text.length() == 0) {
        start = end + 1;
        continue;
      }

      // skip special links
      if (text.indexOf(":") != -1) {
        start = end + 1;
        continue;
      }

      // if there is anchor text, get only article title
      int a;
      if ((a = text.indexOf("|")) != -1) {
        text = text.substring(0, a);
      }

      if ((a = text.indexOf("#")) != -1) {
        text = text.substring(0, a);
      }

      // ignore article-internal links, e.g., [[#section|here]]
      if (text.length() == 0 ) {
        start = end + 1;
        continue;
      }

      links.add(text.trim());

      start = end + 1;
    }

    return links;
  }

  /**
   * Reads a raw XML string into a <code>WikipediaPage</code> object.
   * 
   * @param page
   *            the <code>WikipediaPage</code> object
   * @param s
   *            raw XML string
   */
  public static void readPage(WikipediaPage page, String s) {
    page.page = s;

    // parse out title
    int start = s.indexOf("<title>");
    int end = s.indexOf("</title>", start);
    page.title = StringEscapeUtils.unescapeHtml(s.substring(start + 7, end));

    start = s.indexOf("<id>");
    end = s.indexOf("</id>");
    page.mId = s.substring(start + 4, end);

    // parse out actual text of article
    page.textStart = s.indexOf("<text xml:space=\"preserve\">");
    page.textEnd = s.indexOf("</text>", page.textStart);

    page.isDisambig = s.indexOf("{{disambig", page.textStart) != -1 || s.indexOf("{{Disambig", page.textStart) != -1;
    page.isRedirect = s.substring(page.textStart + 27, page.textStart + 36).compareTo("#REDIRECT") == 0 ||
    s.substring(page.textStart + 27, page.textStart + 36).compareTo("#redirect") == 0;
    page.isStub = s.indexOf("stub}}", page.textStart) != -1;
  }

}
