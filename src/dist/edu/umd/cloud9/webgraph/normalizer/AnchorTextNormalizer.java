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

package edu.umd.cloud9.webgraph.normalizer;

/**
 * 
 * An interface for normalizing lines of anchor text. A Normalizer consists of an stemming, 
 * removing the stop-word, and other processes that lines of anchor text would need to go through,
 * such as converting to lower case, etc. 
 * 
 * @author Nima Asadi
 *
 */

public interface AnchorTextNormalizer {	
	/**
	 * 
	 * Stemmer.
	 * 
	 * @param anchor	original line of anchor text
	 * @return			A line of anchor text where every word from the original anchor text is stemmed.
	 */
	public String stem(String anchor);

	/**
	 * 
	 * @param anchor	original line of anchor text
	 * @return			A line of anchor text where the anchor text is normalized (lower-case, etc.)
	 */
	public String normalize(String anchor);

	/**
	 * 
	 * Removes the stop-words
	 * 
	 * @param anchor	original line of anchor text
	 * @return			A line of anchor text without the stop-words
	 */
	public String removeStopWords(String anchor);

	/**
	 * 
	 * An auxiliary method which performs any other types of normalization.
	 *  
	 * @param anchor	original line of anchor text
	 * @return			A processed line of anchor text.
	 */
	public String process(String anchor);
}
