package edu.umd.cloud9.collection.aquaint2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.Indexable;

public class Aquaint2Document implements Indexable {
	public static final String XML_START_TAG = "<DOC ";
	public static final String XML_END_TAG = "</DOC>";

	private String mRawDoc;
	private String mDocid;
	private String mText;

	private static Pattern sTags = Pattern.compile("<[^>]+>");

	public Aquaint2Document() {
	}

	public void write(DataOutput out) throws IOException {
		byte[] bytes = mRawDoc.getBytes();
		WritableUtils.writeVInt(out, bytes.length);
		out.write(bytes, 0, bytes.length);
	}

	public void readFields(DataInput in) throws IOException {
		int length = WritableUtils.readVInt(in);
		byte[] bytes = new byte[length];
		in.readFully(bytes, 0, length);
		Aquaint2Document.readDocument(this, new String(bytes));
	}

	public String getDocid() {
		if (mDocid == null) {
			int start = 9;
			int end = mRawDoc.indexOf("\"", start);
			mDocid = mRawDoc.substring(start, end).trim();
		}

		return mDocid;
	}

	public String getContent() {
		if (mText == null) {
			int start = mRawDoc.indexOf(">");

			if (start == -1) {
				mText = "";
			} else {
				int end = mRawDoc.length() - 6;
				mText = mRawDoc.substring(start + 1, end).trim();

				mText = sTags.matcher(mText).replaceAll("");
			}
		}

		return mText;
	}

	public static void readDocument(Aquaint2Document doc, String s) {
		if (s == null) {
			throw new RuntimeException("Error, can't read null string!");
		}

		doc.mRawDoc = s;
		doc.mDocid = null;
		doc.mText = null;
	}

}
