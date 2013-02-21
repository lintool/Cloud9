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
public class MixtureModel implements Serializable {

  /**
   * Constant for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Exponential family of the mixture model.
   */
  public ExponentialFamily EF;

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
  public Parameter[] param;

  /**
   * Class constructor.
   * 
   * @param n number of components in the mixture models.
   */
  public MixtureModel(int n) {
    this.EF = null;
    this.size = n;
    this.weight = new double[n];
    this.param = new Parameter[n];
  }

  /**
   * Computes the density value \f$ f(x) \f$ of a mixture model.
   * 
   * @param x a point
   * @return value of the density \f$ f(x) \f$
   */
  public double density(Parameter x) {
    double cumul = 0.0d;
    for (int i = 0; i < this.size; i++)
      cumul += this.weight[i] * this.EF.density(x, this.param[i]);
    return cumul;
  }

  /**
   * Saves a mixture model in a specified output file.
   * 
   * @param mm mixture model to be saved
   * @param fileName file name where the mixture model has to be saved
   */
  public static void save(MixtureModel mm, String fileName) {
    try {

      // Output file and output stream
      FileOutputStream fos = new FileOutputStream(fileName);
      ObjectOutputStream oos = new ObjectOutputStream(fos);

      // Try to write the object
      try {
        oos.writeObject(mm);
        oos.flush();
      } finally {
        try {
          oos.close();
        } finally {
          fos.close();
        }
      }
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
  }

  /**
   * Loads a mixture model from an input file.
   * 
   * @param fileName file name where the mixture model is stored
   * @return mixture model loaded from fileName
   */
  public static MixtureModel load(String fileName) {
    MixtureModel mm = null;
    try {
      // Input file and input stream
      FileInputStream fis = new FileInputStream(fileName);
      ObjectInputStream ois = new ObjectInputStream(fis);
      try {
        mm = (MixtureModel) ois.readObject();
      } finally {
        try {
          ois.close();
        } finally {
          fis.close();
        }
      }
    } catch (IOException ioe) {
      // ioe.printStackTrace();
    } catch (ClassNotFoundException cnfe) {
      // cnfe.printStackTrace();
    }
    return mm;
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
    String output = String.format("Mixture containing %d elements\n\n", size);
    for (int i = 0; i < this.size; i++) {
      output += String.format("Element %4d\n", i);
      output += String.format("Weight:\n %8.6f\n", weight[i]);
      output += String.format("Parameters:\n %s\n\n", param[i]);
    }
    return output;
  }

  /**
   * Creates a sub-mixture by randomly picking components in the instance.
   * 
   * @param m number of components in the the sub mixture
   * @return a sub-mixture of the instance
   */
  public MixtureModel getRandomSubMixtureModel(int m) {
    Random rand = new Random();
    int n = this.size;
    int[] tab = new int[n];
    if (m < n) {
      MixtureModel g = new MixtureModel(m);
      g.EF = this.EF;
      for (int i = 0; i < m; i++) {
        int ind;
        do {
          ind = rand.nextInt(n);
        } while (tab[ind] != 0);
        g.param[i] = this.param[ind].clone();
        g.weight[i] = 1. / m;
        tab[ind] = 1;
      }
      g.normalizeWeights();
      return g;
    } else if (m == n)
      return this;
    else
      return null;
  }

  /**
   * Creates and returns a copy of the instance.
   * 
   * @return a clone of the instance
   */
  public MixtureModel clone() {
    MixtureModel mm = new MixtureModel(this.size);
    mm.EF = this.EF;
    mm.weight = this.weight.clone();
    for (int i = 0; i < this.size; i++)
      mm.param[i] = this.param[i].clone();
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
      points[i] = (PVector) this.EF.drawRandomPoint(this.param[idx]);
    }
    return points;
  }

  /**
   * Estimates the Kullback-Leibler divergence using a Monte-Carlo method.
   * 
   * @param f mixture model
   * @param g mixture model
   * @param n number of points drawn from f
   * @return \f$ D_{\mathrm{KL}}(f\|g)\f$
   */
  public static double KLDMC(MixtureModel f, MixtureModel g, int n) {
    PVector[] points = f.drawRandomPoints(n);
    return KLDMC(f, g, points);
  }

  /**
   * Estimates the Kullback-Leibler divergence using a Monte-Carlo method.
   * 
   * @param f mixture model
   * @param g mixture model
   * @param points points drawn from f
   * @return \f$ D_{\mathrm{KL}}(f\|g)\f$
   */
  public static double KLDMC(MixtureModel f, MixtureModel g, PVector[] points) {

    // Drawn n points in f
    double eps = 10e-100;

    // Estimate DKL
    double kld = 0;
    for (int i = 0; i < points.length; i++)
      kld += Math.log(Math.max(f.density(points[i]), eps) / Math.max(g.density(points[i]), eps));

    // Return DKL
    return kld / points.length;
  }

}