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

package edu.umd.cloud9.collection.wikipedia.language;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import edu.umd.cloud9.collection.wikipedia.WikipediaPage;

/**
 * An Chinese page from Wikipedia.
 * 
 * @author Ferhan Ture
 */
public class ChineseWikipediaPage extends WikipediaPage {
  /**
   * Language dependent identifiers of disambiguation, redirection, and stub pages.
   */
  private static final String IDENTIFIER_REDIRECTION_UPPERCASE = "#REDIRECT";
  private static final String IDENTIFIER_REDIRECTION_LOWERCASE = "#redirect";
  private static final String IDENTIFIER_STUB_TEMPLATE = "stub}}";
  private static final String IDENTIFIER_STUB_WIKIPEDIA_NAMESPACE = "Wikipedia:Stub";
  private static final Pattern disambPattern = Pattern.compile("\\{\\{disambig.+Cat=.+\\}\\}", Pattern.CASE_INSENSITIVE);
  private static final String LANGUAGE_CODE = "zh";

  /**
   * Creates an empty <code>ChineseWikipediaPage</code> object.
   */
  public ChineseWikipediaPage() {
    super();
  }

  @Override
  protected void processPage(String s) {
    this.language = LANGUAGE_CODE;

    // parse out title
    int start = s.indexOf(XML_START_TAG_TITLE);
    int end = s.indexOf(XML_END_TAG_TITLE, start);
    this.title = StringEscapeUtils.unescapeHtml(s.substring(start + 7, end));

    // determine if article belongs to the article namespace
    start = s.indexOf(XML_START_TAG_NAMESPACE);
    end = s.indexOf(XML_END_TAG_NAMESPACE);
    this.isArticle = s.substring(start + 4, end).trim().equals("0");
    
    // parse out the document id
    start = s.indexOf(XML_START_TAG_ID);
    end = s.indexOf(XML_END_TAG_ID);
    this.mId = s.substring(start + 4, end);

    // parse out actual text of article
    this.textStart = s.indexOf(XML_START_TAG_TEXT);
    this.textEnd = s.indexOf(XML_END_TAG_TEXT, this.textStart);

    // determine if article is a disambiguation, redirection, and/or stub page.
    Matcher matcher = disambPattern.matcher(page);
    this.isDisambig = matcher.find();
    this.isRedirect = s.substring(this.textStart + XML_START_TAG_TEXT.length(), this.textStart + XML_START_TAG_TEXT.length() + IDENTIFIER_REDIRECTION_UPPERCASE.length()).compareTo(IDENTIFIER_REDIRECTION_UPPERCASE) == 0 ||
                      s.substring(this.textStart + XML_START_TAG_TEXT.length(), this.textStart + XML_START_TAG_TEXT.length() + IDENTIFIER_REDIRECTION_LOWERCASE.length()).compareTo(IDENTIFIER_REDIRECTION_LOWERCASE) == 0;
    this.isStub = s.indexOf(IDENTIFIER_STUB_TEMPLATE, this.textStart) != -1 || 
                  s.indexOf(IDENTIFIER_STUB_WIKIPEDIA_NAMESPACE) != -1;
  }
}
