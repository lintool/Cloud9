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

package edu.umd.cloud9.demo;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.List;

import edu.uci.ics.jung.algorithms.cluster.WeakComponentGraphClusterer;
import edu.uci.ics.jung.algorithms.importance.PageRank;
import edu.uci.ics.jung.algorithms.importance.Ranking;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import edu.uci.ics.jung.graph.Graph;

/**
 * <p>
 * Program that computes PageRank for a graph using the <a
 * href="http://jung.sourceforge.net/">JUNG</a> package (2.0 alpha1). Program
 * takes two command-line arguments: the first is a file containing the graph
 * data, and the second is the random jump factor (a typical setting is 0.15).
 * </p>
 * 
 * <p>
 * The graph should be represented as an adjacency list. Each line should have
 * at least one token; tokens should be tab delimited. The first token
 * represents the unique id of the source node; subsequent tokens represent its
 * link targets (i.e., outlinks from the source node). For completeness, there
 * should be a line representing all nodes, even nodes without outlinks (those
 * lines will simply contain one token, the source node id).
 * </p>
 * 
 */
public class SequentialPageRank {

	private SequentialPageRank() {
	}

	/**
	 * Runs the program
	 */
	public static void main(String[] args) throws IOException {
		if (args.length != 2) {
			System.err
					.println("usage: SequentialPageRage [graph-adjacency-list] [random-jump-factor]");
			System.exit(-1);
		}
		String infile = args[0];
		float alpha = Float.parseFloat(args[1]);

		int edgeCnt = 0;
		DirectedSparseGraph<String, Integer> graph = new DirectedSparseGraph<String, Integer>();

		BufferedReader data = new BufferedReader(new InputStreamReader(
				new FileInputStream(infile)));

		String line;
		while ((line = data.readLine()) != null) {
			line.trim();
			String[] arr = line.split("\\t");

			for (int i = 1; i < arr.length; i++) {
				graph.addEdge(new Integer(edgeCnt++), arr[0], arr[i]);
			}
		}

		data.close();

		WeakComponentGraphClusterer<String, Integer> clusterer = new WeakComponentGraphClusterer<String, Integer>();

		Collection<Graph<String, Integer>> components = clusterer
				.transform(graph);
		int numComponents = components.size();
		System.out.println("Number of components: " + numComponents);
		System.out.println("Number of edges: " + graph.getEdgeCount());
		System.out.println("Number of nodes: " + graph.getVertexCount());
		System.out.println("Random jump factor: " + alpha);

		PageRank<String, Integer> ranker = new PageRank<String, Integer>(graph,
				alpha);
		ranker.evaluate();

		System.out.println("\nPageRank of nodes, in descending order:");
		for (Ranking<?> s : (List<Ranking<?>>) ranker.getRankings()) {
			String pmid = s.getRanked().toString();

			System.out.println(pmid + " " + s.rankScore);
		}
	}

}
