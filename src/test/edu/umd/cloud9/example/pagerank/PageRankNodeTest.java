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

package edu.umd.cloud9.example.pagerank;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import edu.umd.cloud9.example.pagerank.PageRankNode.Type;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;

public class PageRankNodeTest {

	@Test
	public void testSerialize() throws IOException {
		PageRankNode node1 = new PageRankNode();
		node1.setType(Type.Complete);
		node1.setNodeId(1);
		node1.setPageRank(0.1f);
		node1.setAdjacencyList(new ArrayListOfIntsWritable(new int[] {1,2,3,4,5,6}));

		byte[] bytes = node1.serialize();
		PageRankNode node2 = PageRankNode.create(bytes);

		assertEquals(0.1f, node2.getPageRank(), 10e-6);
		assertEquals(Type.Complete, node2.getType());
		ArrayListOfIntsWritable adj = node2.getAdjacenyList();
		assertEquals(6, adj.size());
		assertEquals(1, adj.get(0));
		assertEquals(2, adj.get(1));
		assertEquals(3, adj.get(2));
		assertEquals(4, adj.get(3));
		assertEquals(5, adj.get(4));
		assertEquals(6, adj.get(5));
	}

	@Test
	public void testToString() throws Exception {
		PageRankNode node = new PageRankNode();
		node.setType(Type.Complete);
		node.setNodeId(1);
		node.setPageRank(0.1f);

		assertEquals("{1 0.1000 []}", node.toString());

		node.setAdjacencyList(new ArrayListOfIntsWritable(new int[] {1,2,3,4,5,6}));
		assertEquals("{1 0.1000 [1, 2, 3, 4, 5, 6]}", node.toString());

		node.setAdjacencyList(new ArrayListOfIntsWritable(new int[] {1,2,3,4,5,6,7,8,9,10,11,12}));
		assertEquals("{1 0.1000 [1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ... (2 more) ]}", node.toString());
	}
	
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PageRankNodeTest.class);
	}
}
