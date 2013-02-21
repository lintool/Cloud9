package edu.umd.cloud9.example.clustering;

import java.util.Random;
import java.util.Set;
import java.util.Vector;

import com.google.common.collect.Sets;

public class ExpectationMaximization1D {

  /**
   * Maximum number of iterations permitted.
   */
  private static int MAX_ITERATIONS = 30;

  /**
   * Initializes a mixture model from clusters of points. The parameters estimated corresponds to
   * univariate Gaussian distributions.
   *
   * @param clusters clusters of points
   * @return mixture model
   */
  public static UnivariateGaussianMixtureModel initialize(Vector<PVector>[] clusters) {
    // Mixture model
    UnivariateGaussianMixtureModel mm = new UnivariateGaussianMixtureModel(clusters.length);

    // Amount of points
    int nb = 0;
    for (int i = 0; i < clusters.length; i++)
      nb += clusters[i].size();

    // Loop on the clusters
    for (int i = 0; i < clusters.length; i++) {

      // Weight
      mm.weight[i] = ((double) clusters[i].size()) / nb;

      // Mean
      double mean = 0;
      for (int j = 0; j < clusters[i].size(); j++) {
        mean += clusters[i].get(j).array[0];
      }
      mean /= clusters[i].size();

      // Variance
      double var = 0;
      for (int j = 0; j < clusters[i].size(); j++) {
        var += (clusters[i].get(j).array[0] - mean) * (clusters[i].get(j).array[0] - mean);
      }
      var /= clusters[i].size();

      // Parameters
      PVector param = new PVector(2);
      param.array[0] = mean;
      param.array[1] = var;
      mm.param[i] = param;
    }

    // Return
    return mm;
  }

  public static UnivariateGaussianMixtureModel initialize(int n) {
    Random rand = new Random();
    // Mixture model
    UnivariateGaussianMixtureModel mm = new UnivariateGaussianMixtureModel(n);

    // Loop on the clusters
    for (int i = 0; i < n; i++) {
      mm.weight[i] = (float) 1/n;
      PVector param = new PVector(2);
      param.array[0] = i*5;
      param.array[1] = 10;
      mm.param[i] = param;
    }

    // Return
    return mm;
  }

  public static Integer[] sampleNUniquePoints(int n, int length) {
    Random rand = new Random();
    Set<Integer> set = Sets.newHashSet();
    while ( set.size() < n ) {
      int r = rand.nextInt(length);
      if (!set.contains(r)) {
        set.add(r);
      }
    }

    return set.toArray(new Integer[set.size()]);
  }

  public static UnivariateGaussianMixtureModel initialize(Point[] points, double[] means) {
    UnivariateGaussianMixtureModel mm = new UnivariateGaussianMixtureModel(means.length);

    for (int i = 0; i < means.length; i++) {
      mm.weight[i] = (float) 1/means.length;
      PVector param = new PVector(2);

      Point tmpPoint = null;
      double minD = Double.MAX_VALUE;
      for (int j=0; j<points.length; j++) {
        double d = Math.abs(points[j].value - means[i]);
        if ( d < minD ) {
          tmpPoint = points[j];
          minD = d;
        }
      }
      param.array[0] = tmpPoint.value;
      param.array[1] = 1;
      mm.param[i] = param;
    }

    // Return
    return mm;
  }

  public static UnivariateGaussianMixtureModel initialize(Point[] points, int n) {
    Random rand = new Random();
    // Mixture model
    UnivariateGaussianMixtureModel mm = new UnivariateGaussianMixtureModel(n);

    Set<Integer> set = Sets.newHashSet();
    while ( set.size() < n ) {
      int r = rand.nextInt(points.length);
      if (!set.contains(r)) {
        set.add(r);
      }
    }

    Integer[] arr = set.toArray(new Integer[set.size()]);
    // Loop on the clusters
    for (int i = 0; i < n; i++) {
      mm.weight[i] = (float) 1/n;
      PVector param = new PVector(2);
      param.array[0] = points[i].value;
      param.array[1] = 1;
      mm.param[i] = param;
    }

    // Return
    return mm;
  }

  /**
   * Performs the Expectation-Maximization algorithm. The parameters estimated corresponds to
   * univariate Gaussian distributions.
   *
   * @param points point set
   * @param m initial mixture model
   * @return mixture model
   */
  public static UnivariateGaussianMixtureModel run(Point[] points, UnivariateGaussianMixtureModel m) {
    UnivariateGaussianMixtureModel mixtureModel = m.clone();

    // Variables
    int numComponents = mixtureModel.size;
    int numPoints = points.length;
    int n, k;
    int iterations = 0;
    double[][] p = new double[numPoints][numComponents];

    // Initial log likelihood
    double logLikelihoodNew = logLikelihood(points, mixtureModel);
    double logLikelihoodThreshold = 10e-10; //Math.abs(logLikelihoodNew) * 0.01;
    double logLikelihoodOld;

    // Display
    System.out.printf("%2d : %12.6f\n", iterations, logLikelihoodNew);

    do {

      logLikelihoodOld = logLikelihoodNew;

      // E-step: computation of matrix P (fast version, we don't compute 1/f(x) for all P[i][j])
      for (n = 0; n < numPoints; n++) {
        double sum = 0;
        for (k = 0; k < numComponents; k++) {
          double tmp = mixtureModel.weight[k] * mixtureModel.densityOfGaussian(points[n], (PVector) mixtureModel.param[k]);
          p[n][k] = tmp;
          sum += tmp;
        }
        for (k = 0; k < numComponents; k++) {
          p[n][k] /= sum;
        }
      }

      // M-step: computation of new Gaussians and the new weights
      for (k = 0; k < numComponents; k++) {

        // Variables
        double sum = 0;
        double mu = 0;
        double sigma = 0;

        // First step of the computation of new mu
        for (n = 0; n < numPoints; n++) {
          double w = p[n][k];
          sum += w;
          mu += points[n].value * w;
        }
        mu /= sum;

        // Computation of new sigma
        for (n = 0; n < numPoints; n++) {
          double diff = points[n].value - mu;
          sigma += p[n][k] * diff * diff;
        }
        sigma /= sum;

        // Set new mu and sigma to the PVectorMatrix
        PVector param = new PVector(2);
        param.array[0] = mu;
        param.array[1] = sigma;
        mixtureModel.param[k] = param;
        mixtureModel.weight[k] = sum / numPoints;
      }

      // Update of iterations and log likelihood value
      iterations++;
      logLikelihoodNew = logLikelihood(points, mixtureModel);

      // Display
      System.out.printf("%2d : %12.6f\n", iterations, logLikelihoodNew);
    } while (Math.abs((logLikelihoodNew - logLikelihoodOld)/logLikelihoodOld) > logLikelihoodThreshold
        && iterations < MAX_ITERATIONS);

    // Return
    return mixtureModel;
  }

  /**
   * Computes the log likelihood.
   *
   * @param points set of points.
   * @param f mixture model.
   * @return log likelihood.
   */
  private static double logLikelihood(Point[] points, UnivariateGaussianMixtureModel f) {
    double value = 0;
    for (int i = 0; i < points.length; i++) {
      //System.out.println(f.density(points[i]));
      value += Math.log(f.density(points[i]));
    }
    return value;
  }
}
