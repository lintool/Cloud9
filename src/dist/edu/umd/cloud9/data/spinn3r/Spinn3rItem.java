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
	private String mGuid;
	private String mLanguage;
	private String mDescription;

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

	public String getDocid() {
		return mGuid;
	}

	public void setDocid(String docid) {
		mGuid = docid;
	}

	public String getContent() {
		return getTitle() + "\n" + getDescription();
	}

	public String getRawXML() {
		return mItem;
	}

	public String getTitle() {
		return mTitle;
	}

	public String getDescription() {
		return mDescription;
	}

	public String getGuid() {
		return mGuid;
	}

	public String getLanguage() {
		return mLanguage;
	}

	public static void readItem(Spinn3rItem item, String s) {
		item.mItem = s;

		// parse out title
		int start = s.indexOf("<title>");
		int end = s.indexOf("</title>", start);
		item.mTitle = s.substring(start + 7, end);

		// parse out guid
		start = s.indexOf("<guid>");
		end = s.indexOf("</guid>", start);
		item.mGuid = s.substring(start + 6, end);

		// parse out actual text of article
		start = s.indexOf("<description>");
		end = s.indexOf("</description>", start);
		item.mDescription = s.substring(start + 13, end);

		start = s.indexOf("<dc:lang>");
		end = s.indexOf("</dc:lang>", start);
		item.mLanguage = s.substring(start + 9, end);

	}

}
