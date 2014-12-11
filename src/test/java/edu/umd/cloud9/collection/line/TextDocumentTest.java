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

package edu.umd.cloud9.collection.line;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class TextDocumentTest {

	@Test
	public void testCreate() {
		TextDocument doc = new TextDocument();

		TextDocument.readDocument(doc, "docid1\tHere are the contents of the document");

		assertEquals("docid1", doc.getDocid());
		assertEquals("Here are the contents of the document", doc.getContent());
	}

	@Test
	public void testSerialization() throws IOException {
		TextDocument doc = new TextDocument();
		TextDocument.readDocument(doc, "docid1\tHere are the contents of the document");

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream dataOut = new DataOutputStream(bytesOut);

		doc.write(dataOut);

		TextDocument doc1 = new TextDocument();

		doc1.readFields(new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray())));

		assertEquals("docid1", doc1.getDocid());
		assertEquals("Here are the contents of the document", doc1.getContent());
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(TextDocumentTest.class);
	}

}
