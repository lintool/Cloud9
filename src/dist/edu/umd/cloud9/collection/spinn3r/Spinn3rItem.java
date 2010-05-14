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

package edu.umd.cloud9.collection.spinn3r;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.Indexable;

public class Spinn3rItem extends Indexable {
	public static final String XML_START_TAG = "<item>";
	public static final String XML_END_TAG = "</item>";

	private String mItem;
	private String mTitle;
	private String mGuid;
	private String mLanguage;
	private String mDescription;
	private Date mPubDate;

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

	public Date getPubDate() {
		if (mPubDate == null) {
			int start = mItem.indexOf("<pubDate>");
			int end = mItem.indexOf("</pubDate>", start);
			String s = mItem.substring(start + 9, end);

			try {
				DateFormat format = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");
				mPubDate = format.parse(s);
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}

		return mPubDate;
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

		item.mPubDate = null;
	}

}
