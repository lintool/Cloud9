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
import java.util.Iterator;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.webgraph.data.AnchorText;

public class AnchorTextTargetTest {

	@Test
	public void testConstructors() {
		AnchorTextTarget anchor = new AnchorTextTarget();
		assertEquals(anchor.getTarget(), 0);
		assertEquals(anchor.getWeight(), 0, 1e-100);
		assertEquals(anchor.getSources().size(), 0);
		
		anchor.addSources(new ArrayListOfIntsWritable(new int[] {1, 2, 3}));
		assertEquals(anchor.getSources().size(), 3);
		
		AnchorTextTarget anchor2 = new AnchorTextTarget(anchor);
		assertEquals(anchor2.getSources().size(), 3);
	}
	
	@Test
	public void testIO() {
		AnchorTextTarget anchor = new AnchorTextTarget();
		anchor.addSources(new ArrayListOfIntsWritable(new int[] {1, 2, 3}));
				
		ByteArrayOutputStream bstream = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bstream);
		
		try {
			anchor.write(out);
			out.close();
		}catch(Exception e) {
		}
		
		DataInputStream in = new DataInputStream(new ByteArrayInputStream(bstream.toByteArray()));
		AnchorTextTarget readAnchor = new AnchorTextTarget();
		try {
			readAnchor.readFields(in);
			in.close();
		}catch(Exception e) {
		}
		
		assertEquals(anchor, readAnchor);
		assertEquals(anchor.getSources().size(), readAnchor.getSources().size());
		
		Iterator<Integer> iterator = readAnchor.iterator();
		while(iterator.hasNext())
			assertTrue(anchor.getSources().contains(iterator.next()));
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(AnchorTextTargetTest.class);
	}
}
