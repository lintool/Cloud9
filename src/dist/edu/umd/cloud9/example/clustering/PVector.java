package edu.umd.cloud9.example.clustering;

import java.util.Arrays;
import java.util.Locale;

/**
 * Parameter vector.
 */
public final class PVector {
  private int dim;
  public double[] array;

  /**
   * Class constructor.
   * 
   * @param dim dimension of the vector
   */
  public PVector(int dim) {
    this.dim = dim;
    this.array = new double[dim];
  }

  public PVector plus(PVector v2) {
    PVector result = new PVector(this.dim);
    PVector q = v2;
    for (int i = 0; i < q.dim; i++)
      result.array[i] = this.array[i] + q.array[i];
    return result;
  }

  public PVector minus(PVector v2) {
    PVector result = new PVector(this.dim);
    PVector q = v2;
    for (int i = 0; i < q.dim; i++)
      result.array[i] = this.array[i] - q.array[i];
    return result;
  }

  public PVector times(double lambda) {
    PVector result = new PVector(this.dim);

    for (int i = 0; i < dim; i++)
      result.array[i] = this.array[i] * lambda;

    return result;
  }

  public double dot(PVector v2) {
    double result = 0.0d;
    PVector q = v2;
    for (int i = 0; i < q.dim; i++)
      result += this.array[i] * q.array[i];
    return result;
  }

  /**
   * Generates of a random vector \f$ v = (x_1, x_2, \cdots )\f$ where each component is drawn
   * uniformly in \f$ \mathcal{U}(0,1)\f$.
   * 
   * @param dim dimension of the vector
   * @return random vector
   */
  public static PVector sampleRandomVector(int dim) {
    PVector result = new PVector(dim);
    for (int i = 0; i < dim; i++)
      result.array[i] = Math.random();
    return result;
  }

  /**
   * Generates of a random vector \f$ v = (x_1, x_2, \cdots )\f$ where each component is drawn
   * uniformly in \f$ \mathcal{U}(0,1)\f$. The vector is normalized such as \f$ \sum_i x_i = 1 \f$.
   * 
   * @param dim dimension of the vector
   * @return random vector
   */
  public static PVector sampleRandomDistribution(int dim) {
    PVector result = sampleRandomVector(dim);
    int i;
    double cumul = 0.0d;
    for (i = 0; i < dim; i++)
      cumul += result.array[i];
    for (i = 0; i < dim; i++)
      result.array[i] /= cumul;
    return result;
  }

  /**
   * Verifies if two vectors are similar.
   * 
   * @param v1 vector \f$ v_1 \f$
   * @param v2 vector \f$ v_2 \f$
   * @return true if \f$ v_1 = v_2 \f$, false otherwise
   */
  public static boolean equals(PVector v1, PVector v2) {
    return Arrays.equals(v1.array, v2.array);
  }

  /**
   * Computes the Euclidean norm of the current vector \f$ v \f$.
   * 
   * @return \f$ \|v\|_2 \f$
   */
  public double norm2() {
    double norm = 0;
    for (int i = 0; i < array.length; i++)
      norm += this.array[i] * this.array[i];
    return Math.sqrt(norm);
  }

  public String toString() {
    String output = "( ";

    for (int i = 0; i < dim; i++)
      output += String.format(Locale.ENGLISH, "%13.6f ", array[i]);

    return output + ")";
  }

  /**
   * Creates and returns a copy of the instance.
   *
   * @return a clone of the instance.
   */
  public PVector clone() {
    PVector param = new PVector(this.dim);
    param.array = this.array.clone();
    return param;
  }

  /**
   * Returns vector's dimension.
   *
   * @return vector's dimension.
   */
  public int getDimension() {
    return this.dim;
  }
}
