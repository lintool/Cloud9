package edu.umd.cloud9.example.graph;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterators;
import it.unimi.dsi.webgraph.NodeIterator;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.ToolRunner;

public class ExtractAdjacencyList {
  private static final String BASE = "base";

  @SuppressWarnings({ "static-access" })
  public static void main(String[] args) throws IOException {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("base prefix").create(BASE));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(BASE) ) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(ExtractAdjacencyList.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String basename = cmdline.getOptionValue(BASE);

    ImmutableGraph graph = BVGraph.load(basename);
    NodeIterator nodeIterator = graph.nodeIterator();

    int numNodes = 0;
    int numEdges = 0;
    while (nodeIterator.hasNext()) {
      numNodes++;
      int cur = nodeIterator.nextInt();
      System.out.print(cur);

      IntIterator successors = LazyIntIterators.eager(graph.successors(cur));
      while (successors.hasNext()) {
        numEdges++;
        System.out.print("\t" + successors.nextInt());
      }
      System.out.print("\n");
    }
    System.err.println("Number of nodes: " + numNodes);
    System.err.println("Number of edges: " + numEdges);
  }
}
