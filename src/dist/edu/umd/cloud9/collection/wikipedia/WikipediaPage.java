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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.Indexable;

/**
 * Object representing an Wikipedia page.
 * 
 * @author Jimmy Lin
 */
public class WikipediaPage implements Indexable {

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
	private int mTextStart;
	private int mTextEnd;
	private boolean mIsRedirect;
	private boolean mIsDisambig;
	private boolean mIsStub;

	/**
	 * Creates an empty <code>WikipediaPage</code> object.
	 */
	public WikipediaPage() {
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
		return getTitle();
	}

	/**
	 * Returns the contents of this page (title + text).
	 */
	public String getContent() {
		return getTitle() + "\n" + getText();
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
	public String getText() {
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

		// parse out actual text of article
		page.mTextStart = s.indexOf("<text xml:space=\"preserve\">");
		page.mTextEnd = s.indexOf("</text>", page.mTextStart);

		page.mIsDisambig = s.indexOf("{{disambig}}", page.mTextStart) != -1;
		page.mIsRedirect = s.substring(page.mTextStart + 27, page.mTextStart + 36).compareTo(
				"#REDIRECT") == 0;
		page.mIsStub = s.indexOf("stub}}", page.mTextStart) != -1;

	}

}
