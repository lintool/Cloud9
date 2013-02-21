package edu.umd.cloud9.example.clustering;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class LocalClusteringDriver {
  private static final Random RANDOM = new Random();

  private static final String POINTS = "points";
  private static final String COMPONENTS = "components";
  private static final String KMEANS = "initializeWithKMeans";
  private static final String HELP = "help";

  @SuppressWarnings({ "static-access" })
  public static void main(String[] args) {
    Options options = new Options();

    options.addOption(new Option(KMEANS, "initialize with k-means"));
    options.addOption(new Option(HELP, "display help options"));

    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("input path").create(POINTS));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("output path").create(COMPONENTS));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
    }

    if (cmdline.hasOption(HELP)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(LocalClusteringDriver.class.getName(), options);
      System.exit(-1);
    }

    int numComponents = cmdline.hasOption(COMPONENTS) ?
        Integer.parseInt(cmdline.getOptionValue(COMPONENTS)) : 3;
    int numPoints = cmdline.hasOption(POINTS) ?
        Integer.parseInt(cmdline.getOptionValue(POINTS)) : 100000;

    System.out.println("Number of points: " + numPoints);
    System.out.println("Number of components in mixture: " + numComponents);
    
    UnivariateGaussianMixtureModel sourceModel = new UnivariateGaussianMixtureModel(numComponents);
    for (int i = 0; i < numComponents; i++) {
      PVector param = new PVector(2);
      param.array[0] = RANDOM.nextInt(100);
      param.array[1] = RANDOM.nextFloat() * 3;
      sourceModel.param[i] = param;
      sourceModel.weight[i] = RANDOM.nextInt(10) + 1;
    }
    sourceModel.normalizeWeights();
    System.out.println("Initial mixture model:\n" + sourceModel + "\n");

    // Draw points from initial mixture model and compute the n clusters
    Point[] points = sourceModel.drawRandomPoints(numPoints);

    UnivariateGaussianMixtureModel learnedModel = null;

    if (cmdline.hasOption(KMEANS)) {
      System.out.println("Running k-means to initialize clusters...");
      List<Point>[] clusters = KMeans.run(points, numComponents);

      double[] means = new double[numComponents];
      int cnt = 0;
      for (List<Point> cluster : clusters) {
        double tmp = 0.0;
        for (Point p : cluster) {
          tmp += p.value;
        }
        means[cnt] = tmp / cluster.size();
        cnt++;
      }

      System.out.println("Cluster means: " + Arrays.toString(means) + "\n");
      learnedModel = ExpectationMaximization.initialize(points, means);
    } else {
      learnedModel = ExpectationMaximization.initialize(points, numComponents);
    }

    System.out.println("** Ready to run EM **\n");
    System.out.println("Initial mixture model:\n" + learnedModel + "\n");

    learnedModel = ExpectationMaximization.run(points, learnedModel);
    System.out.println("Mixure model estimated using EM: \n" + learnedModel + "\n");
  }
}