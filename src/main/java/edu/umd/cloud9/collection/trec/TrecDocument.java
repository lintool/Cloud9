/*
 * Cloud9: A Hadoop toolkit for working with big data
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

import com.google.common.base.Preconditions;

import edu.umd.cloud9.collection.WebDocument;

/**
 * Object representing a TREC document.
 *
 * @author Jimmy Lin
 */
public class TrecDocument extends WebDocument {

  /**
   * Start delimiter of the document, which is &lt;<code>DOC</code>&gt;.
   */
  public static final String XML_START_TAG = "<DOC>";

  /**
   * End delimiter of the document, which is &lt;<code>/DOC</code>&gt;.
   */
  public static final String XML_END_TAG = "</DOC>";

  private String doc;
  private String docid;

  /**
   * Creates an empty {@code TrecDocument} object.
   */
  public TrecDocument() {}

  /**
   * Deserializes this object.
   */
  @Override
  public void write(DataOutput out) throws IOException {
    byte[] bytes = doc.getBytes();
    WritableUtils.writeVInt(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  /**
   * Serializes this object.
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    byte[] bytes = new byte[length];
    in.readFully(bytes, 0, length);
    TrecDocument.readDocument(this, new String(bytes));
  }

  /**
   * Returns the globally-unique String identifier of the document within the collection (e.g.,
   * {@code LA123190-0134}).
   */
  @Override
  public String getDocid() {
    if (docid == null) {
      int start = doc.indexOf("<DOCNO>");

      if (start == -1) {
        docid = "";
      } else {
        int end = doc.indexOf("</DOCNO>", start);
        docid = doc.substring(start + 7, end).trim();
      }
    }

    return docid;
  }

  /**
   * Returns the content of the document.
   */
  @Override
  public String getContent() {
    return doc;
  }

  /**
   * Reads a raw XML string into a {@code TrecDocument} object.
   *
   * @param doc the {@code TrecDocument} object
   * @param s raw XML string
   */
  public static void readDocument(TrecDocument doc, String s) {
    Preconditions.checkNotNull(s);
    Preconditions.checkNotNull(doc);

    doc.doc = s;
    doc.docid = null;
  }

  @Override
  public String getURL() {
    return null;
  }
}
