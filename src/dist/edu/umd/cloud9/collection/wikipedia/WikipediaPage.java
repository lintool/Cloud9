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

import info.bliki.wiki.model.WikiModel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.WritableUtils;
import org.jsoup.Jsoup;
import org.jsoup.helper.StringUtil;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.NodeTraversor;
import org.jsoup.select.NodeVisitor;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import edu.umd.cloud9.collection.Indexable;

/**
 * A page from Wikipedia.
 * 
 * @author Jimmy Lin
 * @author Peter Exner
 */
public abstract class WikipediaPage extends Indexable {
  /**
   * Start delimiter of the page, which is &lt;<code>page</code>&gt;.
   */
  public static final String XML_START_TAG = "<page>";

  /**
   * End delimiter of the page, which is &lt;<code>/page</code>&gt;.
   */
  public static final String XML_END_TAG = "</page>";

  /**
   * Start delimiter of the title, which is &lt;<code>title</code>&gt;.
   */
  protected static final String XML_START_TAG_TITLE = "<title>";

  /**
   * End delimiter of the title, which is &lt;<code>/title</code>&gt;.
   */
  protected static final String XML_END_TAG_TITLE = "</title>";

  /**
   * Start delimiter of the namespace, which is &lt;<code>ns</code>&gt;.
   */
  protected static final String XML_START_TAG_NAMESPACE = "<ns>";

  /**
   * End delimiter of the namespace, which is &lt;<code>/ns</code>&gt;.
   */
  protected static final String XML_END_TAG_NAMESPACE = "</ns>";

  /**
   * Start delimiter of the id, which is &lt;<code>id</code>&gt;.
   */
  protected static final String XML_START_TAG_ID = "<id>";

  /**
   * End delimiter of the id, which is &lt;<code>/id</code>&gt;.
   */
  protected static final String XML_END_TAG_ID = "</id>";

  /**
   * Start delimiter of the text, which is &lt;<code>text xml:space=\"preserve\"</code>&gt;. Note:
   * No close bracket because text element can have multiple attributes.
   */
  protected static final String XML_START_TAG_TEXT = "<text xml:space=\"preserve\"";

  /**
   * End delimiter of the text, which is &lt;<code>/text</code>&gt;.
   */
  protected static final String XML_END_TAG_TEXT = "</text>";

  protected String page;
  protected String title;
  protected String id;
  protected int textStart;
  protected int textEnd;
  protected boolean isRedirect;
  protected boolean isDisambig;
  protected boolean isStub;
  protected boolean isArticle;
  protected String language;

  private WikiModel wikiModel;

  /**
   * Creates an empty <code>WikipediaPage</code> object.
   */
  public WikipediaPage() {
    wikiModel = new WikiModel("", "");
  }

  /**
   * Deserializes this object.
   */
  public void write(DataOutput out) throws IOException {
    byte[] bytes = page.getBytes("UTF-8");
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
    WikipediaPage.readPage(this, new String(bytes, "UTF-8"));
    language = in.readUTF();
  }

  /**
   * Returns the article title (i.e., the docid).
   */
  public String getDocid() {
    return id;
  }

  @Deprecated
  public void setLanguage(String language) {
    this.language = language;
  }

  public String getLanguage() {
    return this.language;
  }

  private static final Pattern REF1 = Pattern.compile("&lt;ref[^/]+/&gt;", Pattern.DOTALL);
  private static final Pattern REF2 = Pattern.compile("&lt;ref.*?&lt;/ref&gt;", Pattern.DOTALL);

  private static final Pattern LANG_LINKS = Pattern.compile("\\[\\[[a-z\\-]+:[^\\]]+\\]\\]");

  private static final Pattern DOUBLE_CURLY = Pattern.compile("\\{\\{.*?\\}\\}", Pattern.DOTALL);

  private static final Pattern HTML_COMMENT = Pattern.compile("(<|&#60;)!--.*?--(>|&#62;)",
      Pattern.DOTALL);

  private static final Pattern EMPTY_PARENS = Pattern.compile(" \\( \\)");

  private static final Pattern FILE = Pattern.compile("\\[\\[(File|Image):.*?\\]\\]");

  /**
   * Returns the contents of this page (title + text).
   */
  public String getContent() {
    String s = getWikiMarkup();
    if (s.length() == 0) {
      return "";
    }

    // Bliki doesn't seem to properly handle inter-language links, so remove manually.
    s = LANG_LINKS.matcher(s).replaceAll(" ");
    // Bliki inlines refs, which we don't want.
    s = REF1.matcher(s).replaceAll("");
    s = REF2.matcher(s).replaceAll("");

    // Known issue: doesn't correctly handle captions that have links inside.
    s = FILE.matcher(s).replaceAll(" ");

    // Known issue: doesn't handle nested {{ .. }} (for example, in infoboxes).
    s = DOUBLE_CURLY.matcher(s).replaceAll(" ");

    wikiModel.setUp();
    s = wikiModel.render(s);
    wikiModel.tearDown();

    s = HTML_COMMENT.matcher(s).replaceAll(" ");

    Document doc = Jsoup.parse(s);

    HtmlToPlainText formatter = new HtmlToPlainText();
    String plainText = formatter.getPlainText(doc);

    plainText = StringEscapeUtils.unescapeHtml(plainText);

    // Take care of things like: id 36
    // '''Albedo''' ({{IPAc-en|icon|æ|l|ˈ|b|iː|d|oʊ}}), or ''reflection coefficient'' ...
    plainText = EMPTY_PARENS.matcher(plainText).replaceAll("");

    return getTitle() + "\n\n" + plainText;
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
    if (textStart == -1 || textStart + 27 > textEnd) {
      // Returning empty string is preferable to returning null to prevent NPE.
      return "";
    }

    return page.substring(textStart + 27, textEnd);
  }

  /**
   * Returns the title of this page.
   */
  public String getTitle() {
    return title;
  }

  /**
   * Checks to see if this page is a disambiguation page. A <code>WikipediaPage</code> is either an
   * article, a disambiguation page, a redirect page, or an empty page.
   * 
   * @return <code>true</code> if this page is a disambiguation page
   */
  public boolean isDisambiguation() {
    return isDisambig;
  }

  /**
   * Checks to see if this page is a redirect page. A <code>WikipediaPage</code> is either an
   * article, a disambiguation page, a redirect page, or an empty page.
   * 
   * @return <code>true</code> if this page is a redirect page
   */
  public boolean isRedirect() {
    return isRedirect;
  }

  /**
   * Checks to see if this page is an empty page. A <code>WikipediaPage</code> is either an article,
   * a disambiguation page, a redirect page, or an empty page.
   * 
   * @return <code>true</code> if this page is an empty page
   */
  public boolean isEmpty() {
    return textStart == -1;
  }

  /**
   * Checks to see if this article is a stub. Return value is only meaningful if this page isn't a
   * disambiguation page, a redirect page, or an empty page.
   * 
   * @return <code>true</code> if this article is a stub
   */
  public boolean isStub() {
    return isStub;
  }

  /**
   * Checks to see if this page lives in the main/article namespace, and not, for example, "File:",
   * "Category:", "Wikipedia:", etc.
   * 
   * @return <code>true</code> if this page is an actual article
   */
  public boolean isArticle() {
    return isArticle;
  }

  /**
   * Returns the inter-language link to a specific language (if any).
   * 
   * @param lang language
   * @return title of the article in the foreign language if link exists, <code>null</code>
   *         otherwise
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

  public static class Link {
    private String anchor;
    private String target;

    private Link(String anchor, String target) {
      this.anchor = anchor;
      this.target = target;
    }

    public String getAnchorText() {
      return anchor;
    }

    public String getTarget() {
      return target;
    }

    public String toString() {
      return String.format("[target: %s, anchor: %s]", target, anchor);
    }
  }

  public List<Link> extractLinks() {
    int start = 0;
    List<Link> links = Lists.newArrayList();

    while (true) {
      start = page.indexOf("[[", start);

      if (start < 0) {
        break;
      }

      int end = page.indexOf("]]", start);

      if (end < 0) {
        break;
      }

      String text = page.substring(start + 2, end);
      String anchor = null;

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
        anchor = text.substring(a + 1, text.length());
        text = text.substring(0, a);
      }

      if ((a = text.indexOf("#")) != -1) {
        text = text.substring(0, a);
      }

      // ignore article-internal links, e.g., [[#section|here]]
      if (text.length() == 0) {
        start = end + 1;
        continue;
      }

      if (anchor == null) {
        anchor = text;
      }
      links.add(new Link(anchor, text));

      start = end + 1;
    }

    return links;
  }

  public List<String> extractLinkTargets() {
    return Lists.transform(extractLinks(), new Function<Link, String>() {
      @Override
      @Nullable
      public String apply(@Nullable Link link) {
        return link.getTarget();
      }
    });
  }

  /**
   * Reads a raw XML string into a <code>WikipediaPage</code> object.
   * 
   * @param page the <code>WikipediaPage</code> object
   * @param s raw XML string
   */
  public static void readPage(WikipediaPage page, String s) {
    page.page = s;
    page.processPage(s);
  }

  /**
   * Reads a raw XML string into a <code>WikipediaPage</code> object. Added for backwards
   * compability.
   * 
   * @param s raw XML string
   */
  protected abstract void processPage(String s);

  // From org.jsoup.examples.HtmlToPlainText
  public static class HtmlToPlainText {
    public String getPlainText(Element element) {
      FormattingVisitor formatter = new FormattingVisitor();
      NodeTraversor traversor = new NodeTraversor(formatter);
      traversor.traverse(element); // walk the DOM, and call .head() and .tail() for each node

      return formatter.toString();
    }

    // the formatting rules, implemented in a breadth-first DOM traverse
    private class FormattingVisitor implements NodeVisitor {
      private StringBuilder accum = new StringBuilder(); // holds the accumulated text

      // hit when the node is first seen
      public void head(Node node, int depth) {
        String name = node.nodeName();
        if (node instanceof TextNode)
          append(((TextNode) node).text()); // TextNodes carry all user-readable text in the DOM.
        else if (name.equals("li"))
          append("\n * ");
      }

      // hit when all of the node's children (if any) have been visited
      public void tail(Node node, int depth) {
        String name = node.nodeName();
        if (name.equals("br"))
          append("\n");
        else if (StringUtil.in(name, "p", "h1", "h2", "h3", "h4", "h5", "table"))
          append("\n\n");
      }

      // appends text to the string builder with a simple word wrap method
      private void append(String text) {
        if (text.equals(" ")
            && (accum.length() == 0 || StringUtil.in(accum.substring(accum.length() - 1), " ", "\n")))
          return; // don't accumulate long runs of empty spaces

        accum.append(text);
      }

      public String toString() {
        return accum.toString();
      }
    }
  }
}
