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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.webgraph.data.AnchorText;

public class AnchorTextTest {

	@Test
	public void testConstructors() {
		AnchorText anchor = new AnchorText();
		assertTrue(anchor.isInternalInLink());
		assertEquals(anchor.getText(), AnchorTextConstants.EMPTY_STRING);
		assertEquals(anchor.getSize(), 0);
		assertEquals(anchor.getWeight(), 0, 1e-100);
		
		AnchorText anchor2 = new AnchorText(AnchorTextConstants.Type.EXTERNAL_IN_LINK.val, "text");
		assertEquals(anchor2.getText(), "text");
		assertEquals(anchor2.getSize(), 0);
		
		AnchorText anchor3 = new AnchorText(AnchorTextConstants.Type.EXTERNAL_OUT_LINK.val, "text");
		assertNull(anchor3.getText());
		assertEquals(anchor3.getSize(), 0);
		
		AnchorText anchor4 = new AnchorText(AnchorTextConstants.Type.DOCNO_FIELD.val, "text", 100);
		assertNull(anchor4.getText());
		assertEquals(anchor4.getSize(), 1);
	}
	
	@Test
	public void testClone() {
		AnchorText anchor1 = new AnchorText(AnchorTextConstants.Type.EXTERNAL_OUT_LINK.val, "text", 1);
		
		AnchorText anchor2 = anchor1.clone();
		anchor2.setText("some text");
		assertTrue(anchor2.equals(anchor1));
		anchor2.addDocument(2);
		assertNull(anchor2.getText());
		assertEquals(anchor2.getSize(), 2);
		assertTrue(anchor2.equalsIgnoreSources(anchor1));
		
		AnchorText anchor3 = new AnchorText(AnchorTextConstants.Type.DOCNO_FIELD.val, "text");
		anchor3.addDocumentsFrom(anchor2);
		anchor3.addDocument(2);
		assertNull(anchor3.getText());
		assertEquals(anchor3.getSize(), 2);
		
		anchor3.setWeight(1);
		assertEquals(anchor3.getWeight(), 0, 1e-100);
		
		assertEquals(anchor3.compareTo(anchor2), 1);
		
		ByteArrayOutputStream bstream = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bstream);
		
		try {
			anchor3.write(out);
			out.close();
		}catch(Exception e) {
		}
		
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(bstream.toByteArray()));
		AnchorText readAnchor = new AnchorText();
		try {
			readAnchor.readFields(in);
			in.close();
		}catch(Exception e) {
		}
		
		assertEquals(anchor3, readAnchor);
				
		assertTrue(anchor3.intersects(anchor2));
		assertTrue(anchor3.containsDocument(2));
		
		anchor3.resetToType(AnchorTextConstants.Type.IN_DEGREE.val);
		assertNull(anchor3.getText());
		
		anchor3.resetToType(AnchorTextConstants.Type.INTERNAL_IN_LINK.val);
		assertEquals(anchor3.getText(), AnchorTextConstants.EMPTY_STRING);
		assertTrue(anchor3.isInternalInLink());
		assertEquals(anchor3.getSize(), 0);
		assertEquals(anchor3.getWeight(), 0, 1e-100);
		assertFalse(anchor3.containsDocument(3));
		assertFalse(anchor3.intersects(anchor2));
		
	}
	
	@Test
	public void testIterable() {
		AnchorText anchor = new AnchorText(AnchorTextConstants.Type.EXTERNAL_IN_LINK.val, "text");
		anchor.addDocument(1);
		anchor.addDocument(2);
		anchor.addDocument(3);
		
		int[] sources = anchor.getDocuments();
		
		assertEquals(sources[0], 1);
		assertEquals(sources[1], 2);
		assertEquals(sources[2], 3);
		
		anchor.resetToType(AnchorTextConstants.Type.URL_FIELD.val);
		assertEquals(anchor.getSize(), 0);
		
		for(@SuppressWarnings("unused") int s : anchor)
			fail();
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(AnchorTextTest.class);
	}
}
