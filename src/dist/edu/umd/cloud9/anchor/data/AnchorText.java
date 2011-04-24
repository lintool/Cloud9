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

package edu.umd.cloud9.anchor.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import bak.pcj.IntIterator;
import bak.pcj.set.IntOpenHashSet;

import org.apache.hadoop.io.WritableComparable;


/**
 * <p>
 * This data structure represents a line of anchor text. A line of anchor text has 
 * some text, a weight, and a set of sources (targets) associated with it. 
 * Sources (targets) are the pages a line of anchor text originates
 * from (points to) when the underlying link is an incoming (outgoing) link.
 * </p>
 * 
 * <p>
 * The implemented iterator makes it possible to iterate through the source or target
 * documents for each line of anchor text.
 * </p>
 * 
 * @author Nima Asadi
 * 
 */


public class AnchorText implements WritableComparable<AnchorText>, AnchorTextConstants, Iterable<Integer> {
	
	private byte flag;		//AnchorText objects can be incoming or outgoing, external or internal, etc. depending on the flag
	
	private String text;	//holds the text for a line of anchor text
	
	private IntOpenHashSet sourceList;	//sources (or targets in case the underlying link is an outgoing one)
	
	private float weight;	//weight for a line of anchor text, if defined
	
	
	/**
	 * Creates an empty Internal Incoming Link AnchorText object
	 */
	public AnchorText() {
		sourceList = new IntOpenHashSet();
	}
	
	/**
	 * Creates a new AnchorText object
	 * 
	 * @param flag
	 * 				Internal or external, incoming or outgoing, etc. (Use AnchorTextConstants interface)
	 * @param text
	 * 				Text associated with a line of anchor text
	 */
	public AnchorText(byte flag, String text) {
		this();
		set(flag, text);
	}
	
	
	/**
	 * Creates a new AnchorText object and adds a new source/target document if 
	 * the AnchorText object is allowed to have text. (Please refer to AnchorTextConstants interface)
	 * 
	 * @param flag
	 * 				Internal or external, incoming or outgoing, etc. (Use AnchorTextConstants interface)
	 * @param text
	 * 				Text associated with a line of anchor text
	 * @param docno
	 * 				Source/Target document id
	 */
	public AnchorText(byte flag, String text, int docno) {
		this();
		set(flag, text);
		
		if(hasValidSourceList())
			addSource(docno);
	}

	/**
	 * Deserializes an AnchorText object.
	 * 
	 * @param in
	 *		     Input Stream
	 */
	public void readFields(DataInput in) throws IOException {
		sourceList.clear();
		flag = in.readByte();
		
		if(hasValidText())
			text = in.readUTF();
		
		if(hasValidSourceList()) {
			int size = in.readInt();
			for(int i = 0; i < size; i++)
				sourceList.add(in.readInt());
		} 
		
		if(hasValidWeight())
			weight = in.readFloat();
		
	}

	/**
	 * Serializes an AnchorText object
	 * 
	 * @param out
	 * 				Output Stream
	 */
	public void write(DataOutput out) throws IOException {
		out.writeByte(flag);
		
		if(hasValidText())
			out.writeUTF(text);
		
		if(hasValidSourceList()) {
			out.writeInt(sourceList.size());
			IntIterator iterator = sourceList.iterator();
			while(iterator.hasNext())
				out.writeInt(iterator.next());
		}
		
		if(hasValidWeight())
			out.writeFloat(weight);
	}
	
	/**
	 * @return the type of this AnchorText
	 */
	public byte getFlag() {
		return flag;
	}
	
	/**
	 * Sets the type of this AnchorText to flag
	 * 
	 * @param flag
	 * 				New type
	 */
	public void setFlag(byte flag) {
		this.flag = flag;
	}
	
	
	/**
	 * @return the text of this line of anchor text.
	 * 			Null if the AnchorText object does not have any text associated with it.
	 */
	public String getText() {
		return text;
	}
	
	/**
	 * Sets the text for this line of anchor text 
	 * @param text
	 * 			New text for this anchor text
	 */
	public void setText(String text) {
		this.text = text;
	}
	
	
	/**
	 * @return the weight for this anchor text
	 */
	public float getWeight() {
		return weight;
	}
	
	/**
	 * Sets a new weight for this line of anchor text
	 * 
	 * @param weight
	 * 			New weight
	 */
	public void setWeight(float weight) {
		this.weight = weight;
		flag = WEIGHTED_EXTERNAL_IN_LINK;
	}
	
	/**
	 * @return the cardinality of the set of sources/targets
	 */
	public int getSize() {
		if(hasValidSourceList())
			return sourceList.size();
		
		return 0;
	}
	
	//returns the list of sources/targets
	private IntOpenHashSet getSourceList() {
		return sourceList;
	}
	
	/**
	 * Returns a list of all the sources/targets
	 * 
	 * @return
	 * 			An array of ints that contains all the sources/targets of the current object
	 */
	public int[] getSources() {
		return sourceList.toArray();
	}
	
	/**
	 * Adds a new source/target to this anchor text.
	 * 
	 * @param docno
	 * 		The new document id to be added to the source/target list
	 */
	public void addSource(int docno) {
		
		if(!hasValidSourceList())
			return;
			
		sourceList.add(docno);
	}
	
	/**
	 * Adds the sources/targets from another AnchorText to the current object
	 * 
	 * @param other
	 * 			The other AnchorText object from which the sources/targets are to be copied.
	 */
	public void addSourcesFrom(AnchorText other) {
		if(!hasValidSourceList())
			return;
		
		IntIterator iterator = other.getSourceList().iterator();
		while(iterator.hasNext())
			addSource(iterator.next());
	}
	
	/**
	 * Checks whether a document is a source or a target for this anchor text
	 * 
	 * @param docno
	 * 			Document to be checked
	 * @return
	 * 			True if the document is a source/target of this anchor text
	 */
	public boolean containsSource(int docno) {
		if(!hasValidSourceList())
			return false;
		
		return sourceList.contains(docno);
	}
	
	/**
	 * Checks whether two lines of anchor text share a source/target document
	 * 
	 * @param other
	 * 			The other anchor text
	 * 
	 * @return
	 * 			True if this line of anchor text has a common source/target
	 * 			with the other line of anchor text.
	 */
	public boolean intersects(AnchorText other) {
		
		if(!hasValidSourceList() || !other.hasValidSourceList())
			return false;
		
		//For efficiency, iterate through the elements of the smallest set
		if(getSize() < other.getSize()) {
			IntIterator iterator = sourceList.iterator();
			while(iterator.hasNext())
				if(other.containsSource(iterator.next()))
					return true;
		}
		else {
			IntIterator iterator = other.getSourceList().iterator();
			while(iterator.hasNext())
				if(sourceList.contains(iterator.next()))
					return true;
		}
			
			
		return false;
	}

	//sets the type of this anchor text and associates some text to it 
	private void set(byte flag, String text) {
		this.flag = flag;
		
		if(hasValidText())
			this.text = text;
	}
	
	/**
	 * Checks whether two lines of anchor text are equal, regardless of their source/target lists.
	 * 
	 * @param other
	 * 			The other anchor text to check against.
	 * 
	 * @return
	 * 			True if the text fields, weights, and types of two AnchorText objects are equal, regardless
	 * 			of their source/targets lists.
	 */
	public boolean equalsIgnoreSources(AnchorText other) {
		
		if(flag != other.getFlag())
			return false;
		
		if(hasValidWeight() && weight != other.getWeight())
			return false;
		
		if(hasValidText())
			return text.equals(other.getText());
		
		return true;
	}
	
	/**
	 * Does a thorough comparison of two AnchorText objects.  
	 */
	public boolean equals(Object obj) {
		AnchorText other = (AnchorText) obj;
		
		if(hasValidSourceList() && other.hasValidSourceList()) {
			if(sourceList.size() != other.getSourceList().size())
				return false;
			
			IntIterator iterator = sourceList.iterator();
			while(iterator.hasNext())
				if(!other.containsSource(iterator.next()))
					return false;
		}
		
		return equalsIgnoreSources(other);
	}
	
	/**
	 * For sorting purposes, the comparison is only limited to the type and the text 
	 * of two AnchorText objects. To check complete equality of the objects use the equals method.
	 */
	public int compareTo(AnchorText obj) {
		
		byte fl = obj.getFlag();
		String text = obj.getText();

		if(flag != fl)
			return flag < fl ? -1 : 1;
		
		if(hasValidText())
			return this.text.compareTo(text);
		
		return 0;
		
	}
	
	public int hashCode() {
		
		if(hasValidText())
			return text.hashCode() + flag;
		
		return flag;
	}
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("(");
		
		if(isExternalInLink())
			builder.append("ExternalInLink");
		else if(isInternalInLink())
			builder.append("InternalInLink");
		else if(isExternalOutLink())
			builder.append("ExternalOutLink");
		else if(isInternalOutLink())
			builder.append("InternalOutLink");
		else if(isDocnoField())
			builder.append("Docno");
		else if(isURL())
			builder.append("URL");
		else if(isInDegree())
			builder.append("Indegree");
		else if(isOutDegree())
			builder.append("Outdegree");
		else
			builder.append("OtherType");
		
		if(hasValidText())
			builder.append(", " + text);
		
		if(hasValidSourceList())
			builder.append(", " + sourceList.toString());
		
		if(hasValidWeight())
			builder.append(", w:" + weight);
		
		builder.append(")");
		
		return builder.toString();
	}
	
	/**
	 * Clones (deep copies) this object and returns a new AnchorText object.
	 * 
	 *  @return
	 *  		A new AnchorText object that is a copy of the current object.
	 */
	public AnchorText clone() {
		AnchorText cloned = new AnchorText();
		
		cloned.set(flag, text);

		if(hasValidSourceList())
			cloned.addSourcesFrom(this);
		
		if(hasValidWeight())
			cloned.weight = weight;
		
		return cloned;
		
	}
	
	
	/**
	 * @return True if the current object is any type of external incoming link (i.e., Weighted, non-weighted, etc.).
	 */
	public boolean isExternalInLink() {
		return flag == EXTERNAL_IN_LINK || flag == WEIGHTED_EXTERNAL_IN_LINK;
	}
	
	/**
	 * @return True if the current object is any type of internal incoming link.
	 */
	public boolean isInternalInLink() {
		return flag == INTERNAL_IN_LINK;
	}
	
	/**
	 * @return True if the current object is any type of external outgoing link.
	 */
	public boolean isExternalOutLink() {
		return flag == EXTERNAL_OUT_LINK;
	}
	
	/**
	 * @return True if the current object is any type of internal outgoing link.
	 */
	public boolean isInternalOutLink() {
		return flag == INTERNAL_OUT_LINK;
	}
	
	/**
	 * @return True if the current line of anchor text has a weight associated with it.
	 */
	public boolean isWeighted() {
		return flag == WEIGHTED_EXTERNAL_IN_LINK;
	}
	
	/**
	 * @return True if the current object is a special type of AnchorText (i.e., is not a real line of anchor text) - InDegree
	 */
	public boolean isInDegree() {
		return flag == IN_DEGREE;
	}
	
	/**
	 * @return True if the current object is a special type of AnchorText (i.e., is not a real line of anchor text) - OutDegree
	 */
	public boolean isOutDegree() {
		return flag == OUT_DEGREE;
	}
	
	/**
	 * @return True if the current object is a special type of AnchorText (i.e., is not a real line of anchor text) - Holds document number
	 */
	public boolean isDocnoField() {
		return flag == DOCNO_FIELD;
	}
	
	/**
	 * @return True if the current object is a special type of AnchorText 
	 * (i.e., is not a real line of anchor text) - Holds a URL address
	 */
	public boolean isURL() {
		return flag == URL_FIELD;
	}
	
	/**
	 * @return True if the current object is a special type of AnchorText 
	 * (i.e., is not a real line of anchor text) - Any other types of AnchorText
	 */
	public boolean isOfOtherTypes() {
		return flag == OTHER_TYPES;
	}
	
	
	//checks whether the anchor text object has a valid text field.
	private boolean hasValidText() {
		
		if(flag == EXTERNAL_IN_LINK || flag == INTERNAL_IN_LINK ||
				flag == URL_FIELD || flag == OTHER_TYPES || 
					flag == WEIGHTED_EXTERNAL_IN_LINK)
			return true;
		
		return false;
	}
	
	//checks whether the current object has a valid list of sources/targets.
	private boolean hasValidSourceList() {
		return flag != URL_FIELD;
	}
	
	//checks wether the current object has a valid weight associated with it.
	private boolean hasValidWeight() {
		return flag == WEIGHTED_EXTERNAL_IN_LINK;
	}

	/**
	 * Creates a new iterator for the current object. The iterator can be used to iterate through
	 * the list of sources/targets associated with the current line of anchor text.
	 * 
	 * @return A new iterator object.
	 */
	public Iterator<Integer> iterator() {
		return new Iterator<Integer>() {
			
			IntIterator iterator = sourceList.iterator();

			public boolean hasNext() {
				return iterator.hasNext();
			}

			public Integer next() {
				return iterator.next();
			}

			public void remove() {
				iterator.remove();
			}
			
		};
	}

}
