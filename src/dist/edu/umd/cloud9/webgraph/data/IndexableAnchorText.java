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

package edu.umd.cloud9.webgraph.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.collection.Indexable;
import edu.umd.cloud9.io.array.ArrayListWritable;

/**
 *
 * An Indexable implementation for anchor text/web graph collections, used in generating ForwardIndex.
 *
 * @author Nima Asadi
 *
 */

public class IndexableAnchorText extends Indexable {
  private static final int DEFAULT_MAX_CONTENT_LENGTH = 1024 * 1024;
  private StringBuilder content;
  private boolean hasDocid = false;
  private String docid = null;

  public IndexableAnchorText() {
    content = new StringBuilder();
    hasDocid = false;
    docid = null;
  }

  public IndexableAnchorText(String docid, ArrayListWritable<AnchorText> anchors) {
    setDocid(docid);
    concatenateAnchors(anchors, DEFAULT_MAX_CONTENT_LENGTH);
  }

  public IndexableAnchorText(String docid, ArrayListWritable<AnchorText> anchors, int maxContentLength) {
    setDocid(docid);
    concatenateAnchors(anchors, maxContentLength);
  }

  public void clear() {
    content.delete(0, content.length());
    hasDocid = false;
    docid = null;
  }

  public void setDocid(String docid) {
    if(docid != null) {
      this.docid = docid;
      hasDocid = true;
    }
  }

  public void concatenateAnchors(ArrayListWritable<AnchorText> anchors) {
    concatenateAnchors(anchors, DEFAULT_MAX_CONTENT_LENGTH);
  }

  public void concatenateAnchors(ArrayListWritable<AnchorText> anchors, int maxContentLength) {
    Preconditions.checkNotNull(anchors);

    content.delete(0, content.length());
    Collections.sort(anchors, new AnchorWeightComparator());

    String previous = "";
    // Concatenate anchors
    for(AnchorText anchor: anchors) {
      if(!anchor.isExternalInLink()) {
        continue;
      }
      String anchorText = anchor.getText();
      if(content.length() + anchorText.length() > maxContentLength) {
        break;
      }
      if(!previous.equals(anchorText)) {
        content.append(anchorText + " ");
        previous = anchorText;
      }
    }
  }

  public void createHTML(ArrayListWritable<AnchorText> anchors) {
    content.delete(0, content.length());

    String url = "";
    for(AnchorText anchor : anchors) {
      if(anchor.isURL()) {
        url = anchor.getText();
      }
    }

    content.append("<html><head><title>" + url + "</title></head><body> Incoming Links:<br />");
    for(AnchorText anchor : anchors) {
      if(anchor.isExternalInLink() || anchor.isInternalInLink()) {
        content.append(anchor.toString() + "<br />");
      }
    }

    content.append("<br />Outgoing Links: <br />");
    for(AnchorText anchor : anchors) {
      if(anchor.isExternalOutLink() || anchor.isInternalOutLink()) {
        content.append(anchor.toString() + "<br />");
      }
    }

    String html = content.toString();
    Matcher m = Pattern.compile("[\\[,]([\\d&&[^,\\[\\]]]*)[,\\]]").matcher(content.toString());
    int start = 0;

    while(m.find(start)) {
      html = html.replace(m.group(), m.group().charAt(0) + "<a href=\"/fetch_docno?docno=" + m.group(1) + "\">" +
              m.group(1) + "</a>" + m.group().charAt(m.group().length() - 1));
      start = m.end() - 1;
    }

    content.delete(0, content.length());
    content.append(html);
  }

  @Override
  public String getContent() {
    return content.toString().trim();
  }

  @Override
  public String getDisplayContentType() {
    return "text/html";
  }

  @Override
  public String getDocid() {
    return docid;
  }

  @Override public String toString() {
    return "Docid: " + docid + "\n" + content.toString();
  }

  public void readFields(DataInput in) throws IOException {
    content.delete(0, content.length());
    docid = null;

    char[] stream = new char[in.readInt()];
    for(int i = 0; i < stream.length; i++) {
      stream[i] = in.readChar();
    }
    content.append(new String(stream));
    hasDocid = in.readBoolean();
    if(hasDocid) {
      docid = in.readUTF();
    }
  }

  public void write(DataOutput out) throws IOException {
    String text = content.toString();
    out.writeInt(text.length());
    out.writeChars(text);
    out.writeBoolean(hasDocid);
    if(hasDocid) {
      out.writeUTF(docid);
    }
  }

  private static class AnchorWeightComparator implements Comparator<AnchorText> {
    public int compare(AnchorText a, AnchorText b) {
      if(a.getType() != b.getType()) {
        return a.getType() < b.getType() ? -1 : 1;
      }

      if(a.isWeighted() && b.isWeighted()) {
        return a.getWeight() > b.getWeight() ? -1 : 1;
      }

      if(a.hasValidText() && b.hasValidText()) {
        return a.getText().compareTo(b.getText());
      }

      return 0;
    }
  }
}
