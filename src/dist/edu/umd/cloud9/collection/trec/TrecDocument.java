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

package edu.umd.cloud9.collection.trec;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.Indexable;

/**
 * Object representing a TREC document.
 * 
 * @author Jimmy Lin
 */
public class TrecDocument extends Indexable {

	/**
	 * Start delimiter of the document, which is &lt;<code>DOC</code>&gt;.
	 */
	public static final String XML_START_TAG = "<DOC>";

	/**
	 * End delimiter of the document, which is &lt;<code>/DOC</code>&gt;.
	 */
	public static final String XML_END_TAG = "</DOC>";

	private String mRawDoc;
	private String mDocid;

	/**
	 * Creates an empty <code>TrecDocument</code> object.
	 */
	public TrecDocument() {
	}

	/**
	 * Deserializes this object.
	 */
	public void write(DataOutput out) throws IOException {
		byte[] bytes = mRawDoc.getBytes();
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
		TrecDocument.readDocument(this, new String(bytes));
	}

	/**
	 * Returns the globally-unique String identifier of the document within the
	 * collection (e.g., <code>LA123190-0134</code>).
	 */
	public String getDocid() {
		if (mDocid == null) {
			int start = mRawDoc.indexOf("<DOCNO>");

			if (start == -1) {
				mDocid = "";
			} else {
				int end = mRawDoc.indexOf("</DOCNO>", start);
				mDocid = mRawDoc.substring(start + 7, end).trim();
			}
		}

		return mDocid;
	}

	/**
	 * Returns the content of the document.
	 */
	public String getContent() {
		return mRawDoc;
	}

	/**
	 * Reads a raw XML string into a <code>TrecDocument</code> object.
	 * 
	 * @param doc
	 *            the <code>TrecDocument</code> object
	 * @param s
	 *            raw XML string
	 */
	public static void readDocument(TrecDocument doc, String s) {
		if (s == null) {
			throw new RuntimeException("Error, can't read null string!");
		}

		doc.mRawDoc = s;
		doc.mDocid = null;
	}
}
