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

package edu.umd.cloud9.collection.line;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.Indexable;

/**
 * Object representing a simple document. Document is encoded as docid followed
 * by tab followed by document contents. Document contents cannot contain
 * embedded tabs or newlines. The entire document is encoded on a single line.
 * 
 * @author Jimmy Lin
 */
public class TextDocument extends Indexable {

	private String mContents;
	private String mDocid;

	/**
	 * Creates an empty <code>TrecDocument</code> object.
	 */
	public TextDocument() {
	}

	/**
	 * Deserializes this object.
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(mDocid);
		byte[] bytes = mContents.getBytes();
		WritableUtils.writeVInt(out, bytes.length);
		out.write(bytes, 0, bytes.length);
	}

	/**
	 * Serializes this object.
	 */
	public void readFields(DataInput in) throws IOException {
		mDocid = in.readUTF();
		int length = WritableUtils.readVInt(in);
		byte[] bytes = new byte[length];
		in.readFully(bytes, 0, length);
		mContents = new String(bytes);
	}

	public String getDocid() {
		return mDocid;
	}

  public void setDocid(String docid) {
    mDocid = docid;
  }

  public String getContent() {
		return mContents;
	}

	public void setContent(String contents) {
	  mContents = contents;
	}
	
	public static void readDocument(TextDocument doc, String s) {
		if (s == null) {
			throw new RuntimeException("Error, can't read null string!");
		}

		String[] arr = s.split("\\t");

		doc.mContents = arr[1];
		doc.mDocid = arr[0];
	}
}
