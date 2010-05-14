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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.Indexable;

/**
 * Object representing an Wikipedia page.
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

	private String mPage;
	private String mTitle;
	private String mId;
	private int mTextStart;
	private int mTextEnd;
	private boolean mIsRedirect;
	private boolean mIsDisambig;
	private boolean mIsStub;

	private WikiModel mWikiModel;
	private PlainTextConverter mTextConverter;
	
	/**
	 * Creates an empty <code>WikipediaPage</code> object.
	 */
	public WikipediaPage() {
        mWikiModel = new WikiModel("", "");
        mTextConverter = new PlainTextConverter();
	}

	/**
	 * Deserializes this object.
	 */
	public void write(DataOutput out) throws IOException {
		byte[] bytes = mPage.getBytes();
		WritableUtils.writeVInt(out, bytes.length);
		out.write(bytes, 0, bytes.length);
	}

	/**
	 * Serializes this object.
	 */
	public void readFields(DataInput in) throws IOException {
		int length = WritableUtils.readVInt(in);
		byte[] bytes = new byte[length];
		in.readFully(bytes, 0, length);
		WikipediaPage.readPage(this, new String(bytes));
	}

	/**
	 * Returns the article title (i.e., the docid).
	 */
	public String getDocid() {
		return mId;
	}

	/**
	 * Returns the contents of this page (title + text).
	 */
	public String getContent() {
		mWikiModel.setUp();
		String s = getTitle() + "\n" + mWikiModel.render(mTextConverter, getWikiMarkup());
		mWikiModel.tearDown();

		// performs some more light cleanup of known Bliki issues
		s = s.replace("&amp;nbsp;", " ");
		s = s.replaceAll("&lt;references */&gt;", "");
		s = s.replaceAll("\\{\\{.*?\\}\\}", "");
		s = s.replaceAll("&#60;ref name.*?&#60;/ref&#62;", "");
		
		// sometimes <ref>http...</ref> appears in the text output
		s = s.replaceAll("&lt;ref&gt;http:.*?&lt;/ref&gt;", "");
		
		return s;
		
		//return getTitle() + "\n" + getWikiMarkup();
		//return parseAndCleanPage2(parseAndCleanPage((getTitle() + "\n" + getText())));
	}
	
	public String getDisplayContent() {
		mWikiModel.setUp();
		String s = "<h1>" + getTitle() + "</h1>\n" + mWikiModel.render(getWikiMarkup());
		mWikiModel.tearDown();
		
		// performs some more light cleanup of known Bliki issues
		s = s.replace("&#38;nbsp;", " ");
		s = s.replace("&#60;references /&#62;", "");
		s = s.replaceAll("\\{\\{.*?\\}\\}", "");
		s = s.replaceAll("&#60;ref name.*?&#60;/ref&#62;", "");
		
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
		return mPage;
	}

	/**
	 * Returns the text of this page.
	 */
	public String getWikiMarkup() {
		if (mTextStart == -1)
			return null;

		return mPage.substring(mTextStart + 27, mTextEnd);
	}

	/**
	 * Returns the title of this page.
	 */
	public String getTitle() {
		return mTitle;
	}

	/**
	 * Checks to see if this page is a disambiguation page. A
	 * <code>WikipediaPage</code> is either an article, a disambiguation page,
	 * a redirect page, or an empty page.
	 * 
	 * @return <code>true</code> if this page is a disambiguation page
	 */
	public boolean isDisambiguation() {
		return mIsDisambig;
	}

	/**
	 * Checks to see if this page is a redirect page. A
	 * <code>WikipediaPage</code> is either an article, a disambiguation page,
	 * a redirect page, or an empty page.
	 * 
	 * @return <code>true</code> if this page is a redirect page
	 */
	public boolean isRedirect() {
		return mIsRedirect;
	}

	/**
	 * Checks to see if this page is an empty page. A <code>WikipediaPage</code>
	 * is either an article, a disambiguation page, a redirect page, or an empty
	 * page.
	 * 
	 * @return <code>true</code> if this page is an empty page
	 */
	public boolean isEmpty() {
		return mTextStart == -1;
	}

	/**
	 * Checks to see if this article is a stub. Return value is only meaningful
	 * if this page isn't a disambiguation page, a redirect page, or an empty
	 * page.
	 * 
	 * @return <code>true</code> if this article is a stub
	 */
	public boolean isStub() {
		return mIsStub;
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
		int start = mPage.indexOf("[[" + lang + ":");

		if (start < 0)
			return null;

		int end = mPage.indexOf("]]", start);

		if (end < 0)
			return null;

		// Some pages have malformed links. For example, "[[de:Frances Willard]"
		// in enwiki-20081008-pages-articles.xml.bz2 has only one closing square
		// bracket. Temporary solution is to ignore malformed links (instead of
		// trying to hack around them).
		String link = mPage.substring(start + 3 + lang.length(), end);

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
			start = mPage.indexOf("[[", start);

			if (start < 0)
				break;

			int end = mPage.indexOf("]]", start);

			if (end < 0)
				break;

			String text = mPage.substring(start + 2, end);

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
		page.mPage = s;

		// parse out title
		int start = s.indexOf("<title>");
		int end = s.indexOf("</title>", start);
		page.mTitle = s.substring(start + 7, end);

		start = s.indexOf("<id>");
		end = s.indexOf("</id>");
		page.mId = s.substring(start + 4, end);

		// parse out actual text of article
		page.mTextStart = s.indexOf("<text xml:space=\"preserve\">");
		page.mTextEnd = s.indexOf("</text>", page.mTextStart);

		page.mIsDisambig = s.indexOf("{{disambig}}", page.mTextStart) != -1;
		page.mIsRedirect = s.substring(page.mTextStart + 27, page.mTextStart + 36).compareTo(
				"#REDIRECT") == 0;
		page.mIsStub = s.indexOf("stub}}", page.mTextStart) != -1;

	}

	private static String parseAndCleanPage2(String raw) {

		// # delete lines in between {{...}}
		// # delete part of line [[ or ]]
		// # delete line starting with *
		//
		// # keep track of open and closed parantheses/brackets for parsing
		int isSkip = 0, count1, count2;

		String[] lines = raw.split("\n");
		// String[] parsed = new String[lines.length];
		String parsed = "";
		boolean isFlag;

		int counter = 0;
		for (String line : lines) {
			isFlag = false;
			// Create a pattern to match cat
			Pattern p1 = Pattern.compile("\\{\\|");
			Matcher m1 = p1.matcher(line);
			Pattern p2 = Pattern.compile("\\|\\}");
			Matcher m2 = p2.matcher(line);

			// Create a matcher with an input string
			if (isSkip == 0) {
				count1 = getCount(m1);

				// isSkip = difference between number of {{s and number of }}s
				if (count1 > 0) {
					count2 = getCount(m2);
					isSkip = count1 - count2;
					isFlag = true;
				}
			} else {
				count1 = getCount(m1);
				count2 = getCount(m2);
				isSkip += (count1 - count2);
				isFlag = true;
			}

			if (isSkip == 0 && !isFlag) {
				// $_=~s/=+//g;
				line = Pattern.compile("```").matcher(line).replaceAll("");
				line = Pattern.compile("\\'\\'\\'").matcher(line).replaceAll("");
				line = Pattern.compile("``").matcher(line).replaceAll("");
				line = Pattern.compile("\\'\\'").matcher(line).replaceAll("");
				line = Pattern.compile("&quot").matcher(line).replaceAll("");
				line = Pattern.compile("\\[http.+\\]").matcher(line).replaceAll("");
				line = Pattern.compile("!--.+--").matcher(line).replaceAll("");

				Matcher mm = Pattern.compile(" (\\S)+\\|(\\S)+ ").matcher(line);
				if (mm.matches()) {
					line = mm.replaceAll(" $2 ");
				}
				// $_=~s/(\w)\s{1}'(\w)/$1'$2/g;
				// $_=~s/(\w)\s{1}'d(\w)/$1'd$2/g;

				line.replaceAll("\\|", " | ");
				if (!Pattern.compile("^\\*.*").matcher(line).matches()
						&& !Pattern.compile("&lt;.*").matcher(line).matches()
						&& !Pattern.compile("&gt;.*").matcher(line).matches()
						&& !Pattern.compile("1\\s+").matcher(line).matches()
						&& !Pattern.compile("\\w\\w:").matcher(line).matches()
						&& !Pattern.compile("^\\s*").matcher(line).matches()
						&& !Pattern.compile("\\=+.+\\=+").matcher(line).matches()
						&& !Pattern.compile("^\\|\\-.+").matcher(line).matches()
						&& !Pattern.compile("Kategorie:.+").matcher(line).matches()
						&& !Pattern.compile("\\w\\w:.+").matcher(line).matches()) {
					parsed += line + "\n";
				}
			}
		}

		return parsed;
	}

	public static String parseAndCleanPage(String raw) {
		String parsed = "";

		// # delete lines in between {{...}}
		// # delete part of line [[ or ]]
		// # delete line starting with *
		//
		// # keep track of open and closed parantheses/brackets for parsing
		int isSkip = 0, count1, count2;

		String[] lines = raw.split("\n");
		boolean isFlag;

		for (String line : lines) {
			isFlag = false;
			// Create a pattern to match cat
			Pattern p1 = Pattern.compile("\\{\\{");
			Matcher m1 = p1.matcher(line);
			Pattern p2 = Pattern.compile("\\}\\}");
			Matcher m2 = p2.matcher(line);

			// Create a matcher with an input string
			if (isSkip == 0) {
				count1 = getCount(m1);
				if (count1 == 0 && line.contains("{{")) {
					throw new RuntimeException();
				}

				// isSkip = difference between number of {{s and number of }}s
				if (count1 > 0) {
					count2 = getCount(m2);
					isSkip = count1 - count2;
					isFlag = true;
				}
			} else {
				count1 = getCount(m1);
				count2 = getCount(m2);

				isSkip += (count1 - count2);
				isFlag = true;
			}

			if (isSkip == 0 && !isFlag) {
				// $_=~s/\[\[//g;
				Pattern p3 = Pattern.compile("\\[\\[");
				Matcher m3 = p3.matcher(line);
				line = m3.replaceAll("");
				//
				// $_=~s/\]\]//g;
				Pattern p4 = Pattern.compile("\\]\\]");
				Matcher m4 = p4.matcher(line);
				line = m4.replaceAll("");
				// $_=~s/=+//g;
				// Pattern p5 = Pattern.compile("\\=+");
				// Matcher m5 = p5.matcher(line);
				// line = m5.replaceAll("");

				// separate sentences # $_=~s/([,;:.)(-])/\n$1\n/g;
				// Pattern p6 = Pattern.compile("([,;.:\\)\\(-])");
				// Matcher m6 = p6.matcher(line);
				// line = m6.replaceAll(" $1");

				// if($_!~/\*/){
				// print $_;
				// }
				Pattern p7 = Pattern.compile("\\*");
				Matcher m7 = p7.matcher(line);
				if (!m7.matches()) {
					parsed += line + "\n";
				}
			}
		}

		return parseAndCleanPage2(parsed);

	}

	static int getCount(Matcher m) {
		int count = 0;
		while (m.find()) {
			count++;
		}
		return count;
	}

}
