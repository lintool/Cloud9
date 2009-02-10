package edu.umd.cloud9.data.wikipedia;

public class WikipediaPage {

	private String mPage;
	private String mTitle;
	private int mTextStart;
	private int mTextEnd;
	private boolean mIsRedirect;
	private boolean mIsDisambig;
	private boolean mIsStub;

	protected WikipediaPage() {
	}

	public String getRawXML() {
		return mPage.toString();
	}

	public String getText() {
		if (mTextStart == -1)
			return null;

		return mPage.substring(mTextStart + 27, mTextEnd);
	}

	public String getTitle() {
		return mTitle;
	}

	public boolean isDisambiguation() {
		return mIsDisambig;
	}

	public boolean isEmpty() {
		return mTextStart == -1;
	}

	public boolean isRedirect() {
		return mIsRedirect;
	}

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

	public static WikipediaPage createEmptyPage() {
		return new WikipediaPage();
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
