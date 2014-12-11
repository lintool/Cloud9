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

package edu.umd.cloud9.collection;

import org.apache.hadoop.io.Writable;

/**
 * A document that can be indexed.
 */
public abstract class Indexable implements Writable {

  /**
   * Returns the globally-unique String identifier of the document within the collection.
   *
   * @return docid of the document
   */
  public abstract String getDocid();

  /**
   * Returns the content of the document.
   *
   * @return content of the document
   */
  public abstract String getContent();

  /**
   * Returns the content of the document for display to a human.
   *
   * @return displayable content
   */
  public String getDisplayContent() {
    return getContent();
  }

  /**
   * Returns the type of the display content, per IANA MIME Media Type (e.g., "text/html").
   * See {@code http://www.iana.org/assignments/media-types/index.html}
   *
   * @return IANA MIME Media Type
   */
  public String getDisplayContentType() {
    return "text/plain";
  }
}
