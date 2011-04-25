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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
	
	public static StringBuilder content = new StringBuilder();
	
	public void process(ArrayListWritable<AnchorText> anchors) {
		
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
		return content.toString();
	}
	
	@Override
	public String getDisplayContentType() {
		return "text/html";
	}

	@Override
	public String getDocid() {
		return null;
	}

	public void readFields(DataInput in) throws IOException {
		content.delete(0, content.length());
		content.append(in.readUTF());
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(content.toString());
	}
}
