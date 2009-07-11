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
