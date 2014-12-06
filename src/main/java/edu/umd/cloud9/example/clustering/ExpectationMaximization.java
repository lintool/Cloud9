package edu.umd.cloud9.example.clustering;

import java.util.Random;
import java.util.Set;

import com.google.common.collect.Sets;

public class ExpectationMaximization {
  // Maximum number of iterations permitted.
  private static int MAX_ITERATIONS = 30;

  /**
   * Initializes the mixture model with points that are closet the given means.
   */
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

    return mm;
  }

  /**
   * Initializes the mixture model with random points.
   */
  public static UnivariateGaussianMixtureModel initialize(Point[] points, int n) {
    UnivariateGaussianMixtureModel mm = new UnivariateGaussianMixtureModel(n);

    Integer[] arr = sampleNUniquePoints(n, points.length);
    for (int i = 0; i < n; i++) {
      mm.weight[i] = (float) 1/n;
      PVector param = new PVector(2);
      param.array[0] = points[arr[i]].value;
      param.array[1] = 1;
      mm.param[i] = param;
    }

    return mm;
  }

  /**
   * Performs the Expectation-Maximization algorithm. The parameters estimated corresponds to
   * univariate Gaussian distributions.
   *
   * @param points point set
   * @param m initial mixture model
   * @return learned mixture model
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

    System.out.printf("Iteration %2d: LL = %12.6f\n", iterations, logLikelihoodNew);

    do {

      logLikelihoodOld = logLikelihoodNew;

      // E-step: computation of matrix P (fast version, we don't compute 1/f(x) for all P[i][j])
      for (n = 0; n < numPoints; n++) {
        double sum = 0;
        for (k = 0; k < numComponents; k++) {
          double tmp = mixtureModel.weight[k] *
              UnivariateGaussianMixtureModel.densityOfGaussian(points[n], mixtureModel.param[k]);
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

        // Set new mu and sigma
        PVector param = new PVector(2);
        param.array[0] = mu;
        param.array[1] = sigma;
        mixtureModel.param[k] = param;
        mixtureModel.weight[k] = sum / numPoints;
      }

      // Update of iterations and log likelihood value
      iterations++;
      logLikelihoodNew = logLikelihood(points, mixtureModel);

      System.out.printf("Iteration %2d: LL = %12.6f\n", iterations, logLikelihoodNew);
    } while (Math.abs((logLikelihoodNew - logLikelihoodOld)/logLikelihoodOld) > logLikelihoodThreshold
        && iterations < MAX_ITERATIONS);

    return mixtureModel;
  }

  /**
   * Computes the log likelihood.
   *
   * @param points set of points
   * @param f mixture model
   * @return log likelihood
   */
  private static double logLikelihood(Point[] points, UnivariateGaussianMixtureModel f) {
    double value = 0;
    for (int i = 0; i < points.length; i++) {
      value += Math.log(f.density(points[i]));
    }
    return value;
  }

  public static final Integer[] sampleNUniquePoints(int n, int length) {
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
}
