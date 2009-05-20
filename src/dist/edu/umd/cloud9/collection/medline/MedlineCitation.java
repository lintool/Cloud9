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

package edu.umd.cloud9.collection.medline;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.Indexable;

/**
 * Object representing a MEDLINE citation.
 * 
 * @author Jimmy Lin
 */
public class MedlineCitation implements Indexable {

	/**
	 * Start delimiter of the document, which is &lt;<code>MedlineCitation</code>
	 * (without closing angle bracket).
	 */
	public static final String XML_START_TAG = "<MedlineCitation";

	/**
	 * End delimiter of the document, which is &lt;<code>/MedlineCitation</code>&gt;.
	 */
	public static final String XML_END_TAG = "</MedlineCitation>";

	private String mPmid;
	private String mCitation;
	private String mTitle;
	private String mAbstract;

	/**
	 * Creates an empty <code>MedlineCitation</code> object.
	 */
	public MedlineCitation() {
	}

	/**
	 * Deserializes this object.
	 */
	public void write(DataOutput out) throws IOException {
		byte[] bytes = mCitation.getBytes();
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
		MedlineCitation.readCitation(this, new String(bytes));
	}

	/**
	 * Returns the docid of this MEDLINE citation, which is its PMID.
	 */
	public String getDocid() {
		return getPmid();
	}

	/**
	 * Returns the content of this citation, which is the title and abstract.
	 */
	public String getContent() {
		return getTitle() + "\n\n" + getAbstract();
	}

	/**
	 * Returns the PMID of this MEDLINE citation.
	 */
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

	/**
	 * Returns the title of this MEDLINE citation.
	 */
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

	/**
	 * Returns the abstract of this citation.
	 */
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

	/**
	 * Returns the raw XML of this citation.
	 */
	public String getRawXML() {
		return mCitation;
	}

	/**
	 * Reads a raw XML string into a <code>MedlineCitation</code> object.
	 * 
	 * @param citation
	 *            the <code>MedlineCitation</code> object
	 * @param s
	 *            raw XML string
	 */
	public static void readCitation(MedlineCitation citation, String s) {
		if (s == null) {
			throw new RuntimeException("Error, can't read null string!");
		}

		citation.mCitation = s;
		citation.mPmid = null;
		citation.mTitle = null;
		citation.mAbstract = null;
	}

}
