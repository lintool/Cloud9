package edu.umd.cloud9.data.spinn3r;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.data.Indexable;

public class Spinn3rItem implements Indexable {
	public static final String XML_START_TAG = "<item>";
	public static final String XML_END_TAG = "</item>";

	private String mItem;
	private String mTitle;
	private String mLanguage;

	public Spinn3rItem() {
	}

	public void write(DataOutput out) throws IOException {
		byte[] bytes = mItem.getBytes();
		WritableUtils.writeVInt(out, bytes.length);
		out.write(bytes, 0, bytes.length);
	}

	public void readFields(DataInput in) throws IOException {
		int length = WritableUtils.readVInt(in);
		byte[] bytes = new byte[length];
		in.readFully(bytes, 0, length);
		Spinn3rItem.readItem(this, new String(bytes));
	}

	public int getDocno() {
		return -1;
	}

	public String getContent() {
		return getTitle();
	}

	public String getRawXML() {
		return mItem;
	}

	public String getTitle() {
		return mTitle;
	}

	public String getLanguage() {
		return mLanguage;
	}

	public static Spinn3rItem createEmptyItem() {
		return new Spinn3rItem();
	}

	public static void readItem(Spinn3rItem item, String s) {
		item.mItem = s;

		// parse out title
		int start = s.indexOf("<title>");
		int end = s.indexOf("</title>", start);
		item.mTitle = s.substring(start + 7, end);

		// parse out actual text of article
		// item.mTextStart = s .indexOf("<text>");
		// item.mTextEnd = s.indexOf("</text>", page.mTextStart);

		start = s.indexOf("<dc:lang>");
		end = s.indexOf("</dc:lang>", start);
		item.mLanguage = s.substring(start + 9, end);

	}

}
