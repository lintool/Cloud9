package edu.umd.cloud9.data.medline;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.data.Indexable;

public class MedlineCitation implements Indexable {

	public static final String XML_START_TAG = "<MedlineCitation";
	public static final String XML_END_TAG = "</MedlineCitation>";

	private String mPmid;
	private String mCitation;
	private String mTitle;
	private String mAbstract;

	public MedlineCitation() {
	}

	public void write(DataOutput out) throws IOException {
		byte[] bytes = mCitation.getBytes();
		WritableUtils.writeVInt(out, bytes.length);
		out.write(bytes, 0, bytes.length);
	}

	public void readFields(DataInput in) throws IOException {
		int length = WritableUtils.readVInt(in);
		byte[] bytes = new byte[length];
		in.readFully(bytes, 0, length);
		MedlineCitation.readPage(this, new String(bytes));
	}

	public String getDocid() {
		return getPmid();
	}

	public String getContent() {
		return getTitle() + "\n\n" + getAbstract();
	}

	public String getPmid() {
		if (mPmid == null) {
			int start = mCitation.indexOf("<PMID>");

			if (start == -1) {
				throw new RuntimeException(getRawXML());
			} else {
				int end = mCitation.indexOf("</PMID>", start);
				mPmid = mCitation.substring(start + 6, end);
			}
		}

		return mPmid;
	}

	public String getTitle() {
		if (mTitle == null) {
			int start = mCitation.indexOf("<ArticleTitle>");

			if (start == -1) {
				mTitle = "";
			} else {
				int end = mCitation.indexOf("</ArticleTitle>", start);
				mTitle = mCitation.substring(start + 14, end);
			}
		}

		return mTitle;
	}

	public String getAbstract() {
		if (mAbstract == null) {
			int start = mCitation.indexOf("<AbstractText>");

			if (start == -1) {
				mAbstract = "";
			} else {
				int end = mCitation.indexOf("</AbstractText>", start);
				mAbstract = mCitation.substring(start + 14, end);
			}
		}

		return mAbstract;
	}

	public String getRawXML() {
		return mCitation;
	}

	public static void readPage(MedlineCitation citation, String s) {
		if (s == null) {
			throw new RuntimeException("Error, can't read null string!");
		}

		citation.mCitation = s;
		citation.mPmid = null;
		citation.mTitle = null;
		citation.mAbstract = null;
	}

}
