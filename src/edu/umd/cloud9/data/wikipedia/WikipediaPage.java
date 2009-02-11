package edu.umd.cloud9.data.wikipedia;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class WikipediaPage implements Writable {
	public static final String XML_START_TAG = "<page>";
	public static final String XML_END_TAG = "</page>";

	private String mPage;
	private String mTitle;
	private int mTextStart;
	private int mTextEnd;
	private boolean mIsRedirect;
	private boolean mIsDisambig;
	private boolean mIsStub;

	public WikipediaPage() {
	}

	public void write(DataOutput out) throws IOException {
		byte[] bytes = mPage.getBytes();
		WritableUtils.writeVInt(out, bytes.length);
		out.write(bytes, 0, bytes.length);
	}

	public void readFields(DataInput in) throws IOException {
		int length = WritableUtils.readVInt(in);
		byte[] bytes = new byte[length];
		in.readFully(bytes, 0, length);
		WikipediaPage.readPage(this, new String(bytes));
	}

	public String getRawXML() {
		return mPage;
	}

	public String getText() {
		if (mTextStart == -1)
			return null;

		return mPage.substring(mTextStart + 27, mTextEnd);
	}

	public String getTitle() {
		return mTitle;
	}

	/**
	 * Checks to see if this page is a disambiguation page. A
	 * <code>WikipediaPage</code> is either an article, a disambiguation page,
	 * a redirect page, or an empty page.
	 * 
	 * @return true if this page is a disambiguation page
	 */
	public boolean isDisambiguation() {
		return mIsDisambig;
	}

	/**
	 * Checks to see if this page is a redirect page. A
	 * <code>WikipediaPage</code> is either an article, a disambiguation page,
	 * a redirect page, or an empty page.
	 * 
	 * @return true if this page is a redirect page
	 */
	public boolean isRedirect() {
		return mIsRedirect;
	}

	/**
	 * Checks to see if this page is an empty page. A <code>WikipediaPage</code>
	 * is either an article, a disambiguation page, a redirect page, or an empty
	 * page.
	 * 
	 * @return true if this page is an empty page
	 */
	public boolean isEmpty() {
		return mTextStart == -1;
	}

	/**
	 * Checks to see if this article is a stub. Return value is only meaningful
	 * if this page isn't a disambiguation page, a redirect page, or an empty
	 * page.
	 * 
	 * @return true if this article is a stub
	 */
	public boolean isStub() {
		return mIsStub;
	}

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
