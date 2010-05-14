package edu.umd.cloud9.collection.pmc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.Indexable;

public class PmcArticle extends Indexable {

	public static final String XML_START_TAG = "<article ";
	public static final String XML_END_TAG = "</article>";

	private String mPmcid;
	private String mDOI;
	private String mArticleText;

	public PmcArticle() {
	}

	public void write(DataOutput out) throws IOException {
		byte[] bytes = mArticleText.getBytes();
		WritableUtils.writeVInt(out, bytes.length);
		out.write(bytes, 0, bytes.length);
	}

	public void readFields(DataInput in) throws IOException {
		int length = WritableUtils.readVInt(in);
		byte[] bytes = new byte[length];
		in.readFully(bytes, 0, length);
		PmcArticle.readArticle(this, new String(bytes));
	}

	public String getDocid() {
		return getPmcid();
	}

	public String getContent() {
		return "";
	}

	public String getPmcid() {
		if (mPmcid == null) {
			int start = mArticleText.indexOf("<article-id pub-id-type=\"pmc\">");

			if (start == -1) {
				throw new RuntimeException(getRawXML());
			} else {
				int end = mArticleText.indexOf("</article-id>", start);
				mPmcid = mArticleText.substring(start + 30, end);
			}
		}

		return mPmcid;
	}

	public String getDOI() {
		if (mDOI == null) {
			int start = mArticleText.indexOf("<article-id pub-id-type=\"doi\">");

			if (start == -1) {
				mDOI = "";
			} else {
				int end = mArticleText.indexOf("</article-id>", start);
				mDOI = mArticleText.substring(start + 30, end);
			}
		}

		return mDOI;
	}

	public String getReferencesXML() {
		int start = mArticleText.indexOf("<ref-list");

		if (start == -1)
			return "";

		int end = mArticleText.indexOf("</ref-list>", start);
		return mArticleText.substring(start, end);
	}

	public String getRawXML() {
		return mArticleText;
	}

	public static void readArticle(PmcArticle article, String s) {
		if (s == null) {
			throw new RuntimeException("Error, can't read null string!");
		}

		article.mArticleText = s;
		article.mPmcid = null;
		article.mDOI = null;
	}

}
