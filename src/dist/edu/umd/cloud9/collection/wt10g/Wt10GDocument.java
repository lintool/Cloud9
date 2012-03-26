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

package edu.umd.cloud9.collection.wt10g;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.WritableUtils;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.collection.Indexable;

/**
 * Object representing a TREC document.
 *
 * @author Jimmy Lin
 */
public class Wt10GDocument extends Indexable
{

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
  private HashMap<String, String> headers;

  /**
   * Creates an empty {@code TrecDocument} object.
   */
  public Wt10GDocument()
  {
    headers = new HashMap<String, String>();
  }

  /**
   * Deserializes this object.
   */
  @Override
  public void write(DataOutput out) throws IOException
  {
    byte[] bytes = doc.getBytes();
    WritableUtils.writeVInt(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }

  /**
   * Serializes this object.
   */
  @Override
  public void readFields(DataInput in) throws IOException
  {
    int length = WritableUtils.readVInt(in);
    byte[] bytes = new byte[length];
    in.readFully(bytes, 0, length);
    Wt10GDocument.readDocument(this, new String(bytes));
  }

  /**
   * Returns the globally-unique String identifier of the document within the
   * collection (e.g., {@code LA123190-0134}).
   */
  @Override
  public String getDocid()
  {
    if (docid == null)
    {
      int start = doc.indexOf("<DOCNO>");

      if (start == -1)
      {
        docid = "";
      } else
      {
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
  public String getContent()
  {
    return doc;
  }

  /**
   * Reads a raw XML string into a {@code TrecDocument} object.
   * 
   * @param doc
   *            the {@code TrecDocument} object
   * @param s
   *            raw XML string
   */
  public static void readDocument(Wt10GDocument doc, String s)
  {
    Preconditions.checkNotNull(s);
    Preconditions.checkNotNull(doc);

    doc.doc = s;
    doc.docid = null;

    // read header
    int start = s.indexOf("<DOCHDR>");

    if (start == -1)
    {
      return;
    } else
    {
      int end = s.indexOf("</DOCHDR>", start);
      String[] header_pieces = s.substring(start + 8, end).trim()
          .toLowerCase().split("\n");
      for (String header : header_pieces)
      {
        if (header.startsWith("http://"))
        {
          doc.headers.put("url", header.split(" ")[0]);
        } else if (!header.contains(":") || header == ""
            || header.length() == 0)
          continue;

        String[] pieces = header.split(":");

        if (pieces.length < 2)
          continue;
        doc.headers.put(pieces[0], pieces[1]);
      }

    }

  }

  public String getHeaderMetadataItem(String string)
  {
    return headers.get(string);
  }
}
