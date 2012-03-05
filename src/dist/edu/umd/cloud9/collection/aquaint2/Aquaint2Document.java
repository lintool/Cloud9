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

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.LineReader;
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

  public static final String AQUAINT_XML_START_TAG = "<DOC>";
  public static final String AQUAINT2_XML_START_TAG = "<DOC ";
  public static final String XML_END_TAG = "</DOC>";

  private String raw;
  private String docid;
  private String headline;
  private String text;
  private boolean isAquaint2Document = true;

  public Aquaint2Document() {
  }


  public static String getXmlStartTag(FileSystem fs, String inputFile) {
    boolean isAquaint2 = true;
    try {
      LineReader reader = new LineReader(fs.open(new Path(inputFile)));
      Text line = new Text();
      reader.readLine(line);
      reader.readLine(line);
      // Aquaint: 'aquaint.dtd'
      // Aquaint2: 'a2_newswire_xml.dtd'
      // Gigaword: 'gigaword.dtd'
      isAquaint2 = ! line.toString().endsWith("'aquaint.dtd'>");
    } catch (IOException e) {
      e.printStackTrace();
    }
    //LOG.info("in getXmlStartTag, isAquaint2: " + isAquaint2);
    if (isAquaint2) {
      return AQUAINT2_XML_START_TAG;
    } else {
      return AQUAINT_XML_START_TAG;
    }
  }


  public static String getXmlEndTag() {
    return XML_END_TAG;
  }


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


  public String getElementText(String elementTagName) {
    String result = "";
    int index = raw.indexOf("<" + elementTagName + ">");
    if (index != -1) {
      int start = index + elementTagName.length() + 2;
      int end = raw.indexOf("</" + elementTagName + ">");
      try {
        result = raw.substring(start, end).trim();
      } catch (Exception e) {
        LOG.error("exception: " + e);
        LOG.error("docid: " + getDocid () + ", index: " + index + ", start: " + start + ", end: " + end);
        LOG.error("raw:\n" + raw);
        result = raw.substring(start).trim();
        LOG.error("found element text: " + result);
        }
      result = TAGS_PATTERN.matcher(result).replaceAll("\n");
      result = WHITESPACE_PATTERN.matcher(result).replaceAll(" ");
      //System.out.println(result);
    }
    return result;
  }


  @Override
  public String getDocid() {
    if (docid == null) {
      if (isAquaint2Document) {
        setAquaint2Docid();
      } else {
        setAquaintDocid();
      }
    }
    return docid;
  }


  private void setAquaintDocid() {
    LOG.trace("setAquaintDocid()");
    docid = getElementText("DOCNO");
    LOG.trace("in setAquaintDocid, docid: " + docid);
  }


  private void setAquaint2Docid() {
    LOG.trace("setAquaint2Docid()");
    int start = 9;
    int end = raw.indexOf("\"", start);
    try {
      docid = raw.substring(start, end).trim();
    } catch (Exception e) {
      LOG.error("exception: " + e);
      LOG.error("start: " + start + ", end: " + end);
      LOG.error("raw:\n" + raw);
      String result = raw.substring(start).trim();
      LOG.error("found element text: " + result);
    }
    LOG.trace("in setAquaint2Docid, docid: " + docid);
  }


  public String getHeadline() {
    if (headline == null) {
      headline = getElementText("HEADLINE");
      if (! isAquaint2Document) {
        headline = getElementText("SLUG").trim().toLowerCase() + ": " + headline;
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
    LOG.trace("readDocument(doc, s), s: \n" + s);
    if (s == null) {
      throw new RuntimeException("Error, can't read null string!");
    }

    doc.raw = s;
    doc.isAquaint2Document = doc.raw.startsWith("<DOC id=");
    doc.docid = null;
    doc.headline = null;
    doc.text = null;

    LOG.debug("docid: " + doc.getDocid() + " length: " + doc.raw.length());
    LOG.trace("readDocument returning");
  }
}
