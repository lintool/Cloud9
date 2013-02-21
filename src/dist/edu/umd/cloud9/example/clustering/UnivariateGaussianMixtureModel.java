package edu.umd.cloud9.example.clustering;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Random;

/**
 * @author Vincent Garcia
 * @author Frank Nielsen
 * @version 1.0
 * 
 * @section License
 * 
 *          See file LICENSE.txt
 * 
 * @section Description
 * 
 *          A mixture model is a powerful framework commonly used to estimate the probability
 *          density function (PDF) of a random variable. Let us consider a mixture model \f$f\f$ of
 *          size \f$n\f$. The probability density function \f$f\f$ evaluated at \f$x \in R^d\f$ is
 *          given by \f[ f(x) = \sum_{i=1}^n \alpha_i f_i(x)\f] where \f$\alpha_i \in [0,1]\f$
 *          denotes the weight of the \f$i^{\textrm{th}}\f$ mixture component \f$f_i\f$ such as
 *          \f$\sum_{i=1}^n \alpha_i=1\f$. The MixtureModel class provides a convenient way to
 *          create and manage mixture of exponential families.
 */
public class UnivariateGaussianMixtureModel {

  /**
   * Exponential family of the mixture model.
   */
  public UnivariateGaussian Gaussian;

  /**
   * Number of components in the mixture model.
   */
  public int size;

  /**
   * Array containing the weights of the mixture components.
   */
  public double[] weight;

  /**
   * Array containing the parameters of the mixture components.
   */
  public PVector[] param;

  /**
   * Class constructor.
   * 
   * @param n number of components in the mixture models.
   */
  public UnivariateGaussianMixtureModel(int n) {
    this.Gaussian = null;
    this.size = n;
    this.weight = new double[n];
    this.param = new PVector[n];
  }

  /**
   * Computes the density value \f$ f(x) \f$ of a mixture model.
   * 
   * @param x a point
   * @return value of the density \f$ f(x) \f$
   */
  public double density(PVector x) {
    double cumul = 0.0d;
    for (int i = 0; i < this.size; i++)
      cumul += this.weight[i] * this.Gaussian.density(x, this.param[i]);
    return cumul;
  }

  /**
   * Normalizes the weights of the mixture models \f$ \alpha_i \f$ such as \f$ \sum_{i=1}^n \alpha_i
   * = 1 \f$.
   */
  public void normalizeWeights() {
    double sum = 0;
    int i;
    for (i = 0; i < this.size; i++)
      sum += this.weight[i];
    for (i = 0; i < this.size; i++)
      this.weight[i] /= sum;
  }

  /**
   * Method toString.
   * 
   * @return string describing the mixture model
   */
  public String toString() {
    String output = String.format("Mixture containing %d components\n", size);
    for (int i = 0; i < this.size; i++) {
      output += String.format("  Component %4d: ", i);
      output += String.format("Weight = %8.6f ", weight[i]);
      output += String.format("Parameters = %s\n", param[i]);
    }
    return output;
  }

  /**
   * Creates and returns a copy of the instance.
   * 
   * @return a clone of the instance
   */
  public UnivariateGaussianMixtureModel clone() {
    UnivariateGaussianMixtureModel mm = new UnivariateGaussianMixtureModel(this.size);
    mm.Gaussian = this.Gaussian;
    mm.weight = this.weight.clone();
    for (int i = 0; i < this.size; i++)
      mm.param[i] = (PVector) this.param[i].clone();
    return mm;
  }

  /**
   * Return the dimension of the parameters of the mixture model.
   * 
   * @return parameters's dimension
   */
  public int getDimension() {
    return this.param[0].getDimension();
  }

  /**
   * Draws points from the considered mixture model.
   * 
   * @param m number of points to draw
   * @return a point
   */
  public PVector[] drawRandomPoints(int m) {

    // Array of points
    PVector[] points = new PVector[m];

    // Cumulative array
    int n = this.size;
    double[] t = new double[n];
    double sum = 0;
    for (int i = 0; i < n; i++) {
      sum += this.weight[i];
      t[i] = sum;
    }

    // Loop
    for (int i = 0; i < m; i++) {

      // Random number between 0 and 1
      double r = Math.random();

      // Find generative class
      int idx = 0;
      while (t[idx] < r && idx < n - 1)
        idx++;

      // Draw and return the point from the idx-th model
      points[i] = (PVector) this.Gaussian.drawRandomPoint(this.param[idx]);
    }
    return points;
  }
}