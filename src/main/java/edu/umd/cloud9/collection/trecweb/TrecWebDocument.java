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

package edu.umd.cloud9.collection.trecweb;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.WebDocument;

public class TrecWebDocument extends WebDocument {

  /**
   * Start delimiter of the document, which is &lt;<code>DOC</code>&gt;.
   */
  public static final String XML_START_TAG = "<DOC>";

  /**
   * End delimiter of the document, which is &lt;<code>/DOC</code>&gt;.
   */
  public static final String XML_END_TAG = "</DOC>";

  private String docid;
  private String content;
  private String url;

  /**
   * Creates an empty <code>Doc2Document</code> object.
   */
  public TrecWebDocument() {
    try {
      startTag = XML_START_TAG.getBytes("utf-8");
      endTag = XML_END_TAG.getBytes("utf-8");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Deserializes this object.
   */
  public void write(DataOutput out) throws IOException {
    out.writeUTF(docid);
    byte[] bytes = content.getBytes("UTF-8");
    WritableUtils.writeVInt(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  /**
   * Serializes this object.
   */
  public void readFields(DataInput in) throws IOException {
    docid = in.readUTF();
    int length = WritableUtils.readVInt(in);
    byte[] bytes = new byte[length];
    in.readFully(bytes, 0, length);
    content = new String(bytes, "UTF-8");
  }

  /**
   * Returns the docid of this Gov2 document.
   */
  @Override
  public String getDocid() {
    return docid;
  }

  /**
   * Returns the content of this Gov2 document.
   */
  @Override
  public String getContent() {
    return content;
  }

  @Override
  public String getURL() {
    return url;
  }

  /**
   * Reads a raw XML string into a {@code TrecWebDocument} object.
   *
   * @param doc the {@code TrecWebDocument} object
   * @param s raw XML string
   */
  public static void readDocument(TrecWebDocument doc, String s) {
    if (s == null) {
      throw new RuntimeException("Error, can't read null string!");
    }

    int start = s.indexOf("<DOCNO>");

    if (start == -1) {
      throw new RuntimeException("Unable to find DOCNO tag!");
    } else {
      int end = s.indexOf("</DOCNO>", start);

      doc.docid = s.substring(start + 7, end);
    }
    
    start = s.indexOf("<DOCHDR>");
    
    if (start == -1) {
      throw new RuntimeException("Unable to find DOCHDR tag!");
    }
    else {
      int end = s.indexOf(" ", start);

      doc.url = s.substring(start + 9, end);
    }
    
    start = s.indexOf("</DOCHDR>");

    if (start == -1) {
      throw new RuntimeException("Unable to find DOCHDR tag!");
    } else {
      int end = s.length() - 6;

      doc.content = s.substring(start + 9, end);
    }

  }

  private static DataInputStream fsin;
  private static byte[] startTag;
  private static byte[] endTag;
  private static DataOutputBuffer buffer = new DataOutputBuffer();

  public static boolean readNextTrecWebDocument(TrecWebDocument doc, DataInputStream stream)
      throws IOException {
    fsin = stream;

    if (readUntilMatch(startTag, false)) {
      try {
        buffer.write(startTag);
        if (readUntilMatch(endTag, true)) {
          String s = new String(buffer.getData());

          readDocument(doc, s);

          return true;
        }
      } finally {
        buffer.reset();
      }
    }

    return false;
  }

  private static boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
    int i = 0;
    while (true) {
      int b = fsin.read();
      // end of file:
      if (b == -1)
        return false;
      // save to buffer:
      if (withinBlock)
        buffer.write(b);

      // check if we're matching:
      if (b == match[i]) {
        i++;
        if (i >= match.length)
          return true;
      } else
        i = 0;
      // see if we've passed the stop point:
      // if (!withinBlock && i == 0 && fsin.getPos() >= end)
      // return false;
    }
  }
}
