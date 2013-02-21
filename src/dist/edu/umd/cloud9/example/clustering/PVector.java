package edu.umd.cloud9.example.clustering;

import java.util.Arrays;
import java.util.Locale;

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
 *          A statistical distribution is parameterized by a set of values (parameters). The PVector
 *          class implements a parameter object. Parameters are represented as a vector.
 */
public final class PVector extends Parameter {

  /**
   * Constant for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Dimension of the vector.
   */
  public int dim;

  /**
   * Array containing the values of the vector.
   */
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

  /**
   * Adds (not in place) the current vector \f$ v_1 \f$ to the vector \f$ v_2 \f$.
   * 
   * @param v2 vector \f$ v_2 \f$
   * @return \f$ v_1 + v_2 \f$
   */
  public PVector Plus(Parameter v2) {
    PVector result = new PVector(this.dim);
    PVector q = (PVector) v2;
    for (int i = 0; i < q.dim; i++)
      result.array[i] = this.array[i] + q.array[i];
    return result;
  }

  /**
   * Subtracts (not in place) the vector \f$ v_2 \f$ to the current vector \f$ v_1 \f$.
   * 
   * @param v2 vector \f$ v_2 \f$
   * @return \f$ v_1 - v_2 \f$
   */
  public PVector Minus(Parameter v2) {
    PVector result = new PVector(this.dim);
    PVector q = (PVector) v2;
    for (int i = 0; i < q.dim; i++)
      result.array[i] = this.array[i] - q.array[i];
    return result;
  }

  /**
   * Multiplies (not in place) the current vector \f$ v \f$ by a real number \f$ \lambda \f$.
   * 
   * @param lambda value \f$ \lambda \f$
   * @return \f$ \lambda . v\f$
   */
  public PVector Times(double lambda) {
    PVector result = new PVector(this.dim);

    for (int i = 0; i < dim; i++)
      result.array[i] = this.array[i] * lambda;

    return result;
  }

  /**
   * Computes the inner product (real number) between the current vector \f$ v_1 \f$ and the vector
   * \f$ v_2 \f$.
   * 
   * @param v2 vector \f$ v_2 \f$
   * @return \f$ v_1^\top . v_2 \f$
   */
  public double InnerProduct(Parameter v2) {
    double result = 0.0d;
    PVector q = (PVector) v2;
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
  public static PVector Random(int dim) {
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
  public static PVector RandomDistribution(int dim) {
    PVector result = Random(dim);
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

  /**
   * Method toString.
   * 
   * @return value of the vector as a string
   */
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
  public Parameter clone() {
    PVector param = new PVector(this.dim);
    param.type = this.type;
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
