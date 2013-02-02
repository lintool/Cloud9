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

/**
 * <p>
 * This interface holds the constants used in the AnchorText data structure.
 * </p>
 *
 * @author Nima Asadi
 *
 */

public interface AnchorTextConstants {
  public static final String EMPTY_STRING = "";

  public static enum Type {
    INTERNAL_IN_LINK ((byte) 0),  //Indicates an Internal Incoming Link
    EXTERNAL_IN_LINK ((byte) 1),  //Indicates an External Incoming Link
    INTERNAL_OUT_LINK ((byte) 2),  //Indicates an Internal Outgoing Link
    EXTERNAL_OUT_LINK ((byte) 3),  //Indicates an External Outgoing Link
    URL_FIELD ((byte) 4), //Indicates whether the data structure is
                    //only used to hold a URL address
    DOCNO_FIELD ((byte) 5),      //Indicates whether the data structure is
                    //only used to hold a document number/id
    OUT_DEGREE ((byte) 6),       //Indicates whether the data structure is
                    //only used to hold the outdegree for a document
    IN_DEGREE ((byte) 7),      //Indicates whether the data structure is
                    //only used to hold the indegree for a document
    OTHER_TYPES ((byte) 8),      //Indicates whether the data structure is only
                    //used to hold other types of information
    WEIGHTED_EXTERNAL_IN_LINK ((byte) 11);  //Indicates a weighted External Incoming Link

    public byte val;
    private Type(byte v) {
      this.val = v;
    }
  }

  /**
   * Shows the maximum number of sources/targets an AnchorText object can hold.
   * If the number of sources/targets is greater or equal to this value, the
   * AnchorText object is split into smaller objects.
   */
  public static final int MAXIMUM_SOURCES_PER_PACKET = 1*1024*1024;
}
