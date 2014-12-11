/*
 * Cloud9: A Hadoop toolkit for working with big data
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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.collections15.Transformer;
import org.apache.hadoop.util.ToolRunner;

import edu.uci.ics.jung.algorithms.cluster.WeakComponentClusterer;
import edu.uci.ics.jung.algorithms.importance.Ranking;
import edu.uci.ics.jung.algorithms.scoring.PageRankWithPriors;
import edu.uci.ics.jung.graph.DirectedSparseGraph;

/**
 * <p>
 * Program that computes personalized PageRank for a graph using the <a
 * href="http://jung.sourceforge.net/">JUNG</a> package (2.0 alpha1). Program takes two command-line
 * arguments: the first is a file containing the graph data, and the second is the random jump
 * factor (a typical setting is 0.15).
 * </p>
 *
 * <p>
 * The graph should be represented as an adjacency list. Each line should have at least one token;
 * tokens should be tab delimited. The first token represents the unique id of the source node;
 * subsequent tokens represent its link targets (i.e., outlinks from the source node). For
 * completeness, there should be a line representing all nodes, even nodes without outlinks (those
 * lines will simply contain one token, the source node id).
 * </p>
 *
 * @author Jimmy Lin
 */
public class SequentialPersonalizedPageRank {
  private SequentialPersonalizedPageRank() {}

  private static final String INPUT = "input";
  private static final String JUMP = "jump";
  private static final String SOURCE = "source";

  @SuppressWarnings({ "static-access" })
  public static void main(String[] args) throws IOException {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("val").hasArg()
        .withDescription("random jump factor").create(JUMP));
    options.addOption(OptionBuilder.withArgName("node").hasArg()
        .withDescription("source node (i.e., destination of the random jump)").create(SOURCE));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(SOURCE)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(SequentialPersonalizedPageRank.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String infile = cmdline.getOptionValue(INPUT);
    final String source = cmdline.getOptionValue(SOURCE);
    float alpha = cmdline.hasOption(JUMP) ? Float.parseFloat(cmdline.getOptionValue(JUMP)) : 0.15f;

    int edgeCnt = 0;
    DirectedSparseGraph<String, Integer> graph = new DirectedSparseGraph<String, Integer>();

    BufferedReader data = new BufferedReader(new InputStreamReader(new FileInputStream(infile)));

    String line;
    while ((line = data.readLine()) != null) {
      line.trim();
      String[] arr = line.split("\\t");

      for (int i = 1; i < arr.length; i++) {
        graph.addEdge(new Integer(edgeCnt++), arr[0], arr[i]);
      }
    }

    data.close();

    if (!graph.containsVertex(source)) {
      System.err.println("Error: source node not found in the graph!");
      System.exit(-1);
    }

    WeakComponentClusterer<String, Integer> clusterer = new WeakComponentClusterer<String, Integer>();

    Set<Set<String>> components = clusterer.transform(graph);
    int numComponents = components.size();
    System.out.println("Number of components: " + numComponents);
    System.out.println("Number of edges: " + graph.getEdgeCount());
    System.out.println("Number of nodes: " + graph.getVertexCount());
    System.out.println("Random jump factor: " + alpha);

    // Compute personalized PageRank.
    PageRankWithPriors<String, Integer> ranker = new PageRankWithPriors<String, Integer>(graph,
        new Transformer<String, Double>() {
          @Override
          public Double transform(String vertex) {
            return vertex.equals(source) ? 1.0 : 0;
          }
        }, alpha);
    
    ranker.evaluate();

    // Use priority queue to sort vertices by PageRank values.
    PriorityQueue<Ranking<String>> q = new PriorityQueue<Ranking<String>>();
    int i = 0;
    for (String pmid : graph.getVertices()) {
      q.add(new Ranking<String>(i++, ranker.getVertexScore(pmid), pmid));
    }

    // Print PageRank values.
    System.out.println("\nPageRank of nodes, in descending order:");
    Ranking<String> r = null;
    while ((r = q.poll()) != null) {
      System.out.println(r.rankScore + "\t" + r.getRanked());
    }
  }
}
