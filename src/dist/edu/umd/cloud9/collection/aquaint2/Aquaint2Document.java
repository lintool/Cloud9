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
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.Indexable;


public class Aquaint2Document extends Indexable {
  private static final Logger LOG = Logger.getLogger(Aquaint2Document.class);
  {
    LOG.setLevel(Level.INFO);
    //LOG.setLevel(Level.TRACE);
  }

  private static Pattern TAGS_PATTERN = Pattern.compile("<[^>]+>");
  private static Pattern WHITESPACE_PATTERN = Pattern.compile("\t|\n");

  public static final String XML_START_TAG = "<DOC";
  public static final String XML_END_TAG = "</DOC>";

  private boolean isAquaint2;
  private String raw;
  private String docid;
  private String headline;
  private String text;

  public Aquaint2Document() {}


  @Override
  public void write(DataOutput out) throws IOException {
    byte[] bytes = raw.getBytes();
    WritableUtils.writeVInt(out, bytes.length);
    out.write(bytes, 0, bytes.length);
  }


  @Override
  public void readFields(DataInput in) throws IOException {
    int length = WritableUtils.readVInt(in);
    byte[] bytes = new byte[length];
    in.readFully(bytes, 0, length);
    Aquaint2Document.readDocument(this, new String(bytes));
  }


  @Override
  public String getDocid() {
    if (docid == null) {
      if (isAquaint2) {
        setAquaint2Docid();
      } else {
        setAquaintDocid();
      }
    }
    return docid;
  }


  private void setAquaintDocid() {
    int start = raw.indexOf("<DOCNO>");
    if (start == -1) {
      docid = "";
    } else {
      int end = raw.indexOf("</DOCNO>");
      docid = raw.substring(start + 7, end).trim();
    }
    LOG.trace("in setAquaintDocid, docid: " + docid);
  }


  private void setAquaint2Docid() {
    int start = 9;
    int end = raw.indexOf("\"", start);
    docid = raw.substring(start, end).trim();
    LOG.trace("in setAquaint2Docid, docid: " + docid);
  }


  public String getHeadline() {
    if (headline == null) {
      int start = raw.indexOf("<HEADLINE>");
      if (start == -1) {
        headline = "";
      } else {
        int end = raw.indexOf("</HEADLINE>");
        try {
          headline = raw.substring(start + 10, end).trim();
        } catch (Exception e) {
          LOG.error("exception: " + e);
          LOG.error("docid: " + getDocid () + ", start: " + start + ", end: " + end);
          LOG.error("raw:\n" + raw);
          headline = raw.substring(start + 10).trim();
          LOG.error("updated headline: " + headline);
        }
        headline = TAGS_PATTERN.matcher(headline).replaceAll("");
        headline = WHITESPACE_PATTERN.matcher(headline).replaceAll(" ");
      }
    }
    return headline;
  }


  @Override
  public String getContent() {
    if (text == null) {
      int start = raw.indexOf(">");

      if (start == -1) {
        text = "";
      } else {
        int end = raw.length() - 6;
        text = raw.substring(start + 1, end).trim();

        text = TAGS_PATTERN.matcher(text).replaceAll("");
      }
    }
    return text;
  }


  public static void readDocument(Aquaint2Document doc, String s) {
    if (s == null) {
      throw new RuntimeException("Error, can't read null string!");
    }

    doc.raw = s;
    doc.docid = null;
    doc.headline = null;
    doc.text = null;
    //doc.isAquaint2 = (doc.raw.indexOf("</DOCNO>\n<DOCTYPE>") == -1);
    doc.isAquaint2 = (doc.raw.indexOf("<DOCNO>") == -1);

    LOG.debug("docid: " + doc.getDocid() + " length: " + doc.raw.length());
  }
}
