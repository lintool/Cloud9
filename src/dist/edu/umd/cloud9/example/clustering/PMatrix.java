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
 *          A statistical distribution is parameterized by a set of values (parameters). The PMatrix
 *          class implements a parameter object. Parameters are represented as a matrix.
 */
public class PMatrix extends Parameter {

  /**
   * Constant for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Dimension of the matrix.
   */
  public int dim;

  /**
   * Array containing the values of the matrix.
   */
  public double[][] array;

  /**
   * Class constructor.
   * 
   * @param dim dimension of the matrix
   */
  public PMatrix(int dim) {
    this.dim = dim;
    this.array = new double[dim][dim];
  }

  /**
   * Class constructor by copy.
   * 
   * @param M matrix to copy
   */
  public PMatrix(PMatrix M) {
    this.dim = M.dim;
    this.array = new double[this.dim][this.dim];

    for (int i = 0; i < this.dim; i++)
      for (int j = 0; j < this.dim; j++)
        this.array[i][j] = M.array[i][j];
  }

  /**
   * Adds (not in place) the current matrix \f$ m_1 \f$ to the matrix \f$ m_2 \f$.
   * 
   * @param m2 matrix \f$ m_2 \f$
   * @return \f$ m_1 + m_2 \f$
   */
  public PMatrix Plus(Parameter m2) {
    PMatrix Q = (PMatrix) m2;
    PMatrix result = new PMatrix(this.dim);
    for (int i = 0; i < this.dim; i++)
      for (int j = 0; j < this.dim; j++)
        result.array[i][j] = this.array[i][j] + Q.array[i][j];
    return result;
  }

  /**
   * Subtracts (not in place) the matrix \f$ m_2 \f$ to the current matrix \f$ v_1 \f$.
   * 
   * @param m2 vector \f$ m_2 \f$
   * @return \f$ m_1 - m_2 \f$
   */
  public PMatrix Minus(Parameter m2) {
    PMatrix Q = (PMatrix) m2;
    PMatrix result = new PMatrix(this.dim);
    for (int i = 0; i < this.dim; i++)
      for (int j = 0; j < this.dim; j++)
        result.array[i][j] = this.array[i][j] - Q.array[i][j];
    return result;
  }

  /**
   * Multiplies (not in place) the current matrix \f$ m \f$ by a real number \f$ \lambda \f$.
   * 
   * @param lambda value \f$ \lambda \f$
   * @return \f$ \lambda m\f$
   */
  public PMatrix Times(double lambda) {
    PMatrix result = new PMatrix(this.dim);
    for (int i = 0; i < this.dim; i++)
      for (int j = 0; j < this.dim; j++)
        result.array[i][j] = this.array[i][j] * lambda;
    return result;
  }

  /**
   * Computes the inner product (real number) between the current matrix \f$ m_1 \f$ and the matrix
   * \f$ m_2 \f$.
   * 
   * @param m2 vector \f$ m_2 \f$
   * @return \f$ tr(m_1 . m_2^\top) \f$
   */
  public double InnerProduct(Parameter m2) {
    PMatrix Q = (PMatrix) m2;
    return (this.Multiply(Q.Transpose())).Trace();
  }

  /**
   * Multiplies (not in place) the current matrix \f$ v_1 \f$ by the matrix \f$ m_2 \f$.
   * 
   * @param m2 matrix \f$ m_2 \f$
   * @return \f$ m_1 m_2\f$
   */
  public PMatrix Multiply(PMatrix m2) {
    PMatrix result = new PMatrix(this.dim);
    double sum;
    for (int i = 0; i < this.dim; i++)
      for (int j = 0; j < this.dim; j++) {
        sum = 0.0d;
        for (int k = 0; k < this.dim; k++)
          sum += this.array[i][k] * m2.array[k][j];
        result.array[i][j] = sum;
      }
    return result;
  }

  /**
   * Multiplies (not in place) the current matrix \f$ m \f$ by a vector \f$ v \f$.
   * 
   * @param v vector \f$ v \f$
   * @return \f$ m . v\f$
   */
  public PVector MultiplyVectorRight(PVector v) {
    PVector result = new PVector(v.dim);
    double sum;
    for (int i = 0; i < this.dim; i++) {
      sum = 0.0d;
      for (int j = 0; j < this.dim; j++)
        sum += this.array[i][j] * v.array[j];
      result.array[i] = sum;
    }
    return result;
  }

  /**
   * Computes the inverse of the current matrix \f$ m \f$ using Gauss-Jordan elimination.
   * 
   * @return \f$ m^{-1} \f$
   */
  public PMatrix Inverse() {
    PMatrix result = new PMatrix(this);
    GaussJordan(result.array, this.dim);
    return result;
  }

  /**
   * Gauss-Jordan elimination.
   * 
   * @param a matrix to inverse
   * @param dim dimension of the matrix
   */
  private static void GaussJordan(double a[][], int dim) {
    double det = 1.0d, big, save;
    int i, j, k, L;
    int[] ik = new int[dim];
    int[] jk = new int[dim];
    for (k = 0; k < dim; k++) {
      big = 0.0d;
      for (i = k; i < dim; i++)
        for (j = k; j < dim; j++)
          // find biggest element
          if (Math.abs(big) <= Math.abs(a[i][j])) {
            big = a[i][j];
            ik[k] = i;
            jk[k] = j;
          }
      if (big == 0.0) {
        // NOT INVERTIBLE!!!
        // Frank: Raise exception
      }
      i = ik[k];
      if (i > k)
        for (j = 0; j < dim; j++) { // exchange rows
          save = a[k][j];
          a[k][j] = a[i][j];
          a[i][j] = -save;
        }
      j = jk[k];
      if (j > k)
        for (i = 0; i < dim; i++) {
          save = a[i][k];
          a[i][k] = a[i][j];
          a[i][j] = -save;
        }
      for (i = 0; i < dim; i++)
        // build the inverse
        if (i != k)
          a[i][k] = -a[i][k] / big;
      for (i = 0; i < dim; i++)
        for (j = 0; j < dim; j++)
          if ((i != k) && (j != k))
            a[i][j] += a[i][k] * a[k][j];
      for (j = 0; j < dim; j++)
        if (j != k)
          a[k][j] /= big;
      a[k][k] = 1.0 / big;
      det *= big; // bomb point
    } // end k loop
    for (L = 0; L < dim; L++) {
      k = dim - L - 1;
      j = ik[k];
      if (j > k)
        for (i = 0; i < dim; i++) {
          save = a[i][k];
          a[i][k] = -a[i][j];
          a[i][j] = save;
        }
      i = jk[k];
      if (i > k)
        for (j = 0; j < dim; j++) {
          save = a[k][j];
          a[k][j] = -a[i][j];
          a[i][j] = save;
        }
    }
  }

  /**
   * Transposes the current matrix \f$ m \f$.
   * 
   * @return \f$ m^\top \f$
   */
  public PMatrix Transpose() {
    PMatrix T = new PMatrix(this.dim);
    for (int i = 0; i < this.dim; i++)
      for (int j = 0; j < this.dim; j++)
        T.array[i][j] = this.array[j][i];
    return T;
  }

  /**
   * Computes the determinant of the current matrix \f$ m \f$.
   * 
   * @return \f$ \det (m)\f$
   */
  public double Determinant() {
    double result = 0.0d;
    if (this.dim == 1)
      return array[0][0];
    PMatrix SubMatrix = new PMatrix(this.dim - 1);
    for (int i = 0; i < this.dim; i++) {
      for (int j = 1; j < this.dim; j++) {
        for (int k = 0; k < this.dim; k++) {
          if (k < i)
            SubMatrix.array[j - 1][k] = array[j][k];
          else if (k > i)
            SubMatrix.array[j - 1][k - 1] = array[j][k];
        }
      }
      result += array[0][i] * Math.pow(-1, (double) i) * SubMatrix.Determinant();
    }
    return result;
  }

  /**
   * Computes the trace of the current matrix \f$ m \f$.
   * 
   * @return \f$ tr (m)\f$
   */
  public double Trace() {
    double tr = 0.0d;
    for (int i = 0; i < this.dim; i++)
      tr += this.array[i][i];
    return tr;
  }

  /**
   * Generates a random matrix \f$ m \f$ where each element is drawn from \f$ \mathcal{U}(0,1)\f$.
   * 
   * @param dim dimension of the matrix
   * @return random matrix \f$ m \f$
   */
  public static PMatrix Random(int dim) {
    PMatrix m = new PMatrix(dim);
    for (int i = 0; i < dim; i++)
      for (int j = 0; j < dim; j++)
        m.array[i][j] = Math.random();
    return m;
  }

  /**
   * Generates a random matrix \f$ m \f$ such as \f$ m \f$ is a positive definite matrix: Draw a
   * lower triangular matrix \f$ L \f$ at random and then return \f$ LL^T\f$.
   * 
   * @param dim dimension of the matrix
   * @return random matrix \f$ m = L L^T\f$
   */
  public static PMatrix RandomPositiveDefinite(int dim) {
    PMatrix L = new PMatrix(dim);
    for (int i = 0; i < dim; i++)
      for (int j = 0; j < dim; j++) {
        if (j >= i)
          L.array[i][j] = Math.random();
        else
          L.array[i][j] = 0.0;
      }
    return L.Multiply(L.Transpose());
  }

  /**
   * Computes the Cholesky decomposition of the current matrix \f$ m \f$.
   * 
   * @return a lower triangular matrix
   */
  public PMatrix Cholesky() {
    PMatrix L = new PMatrix(this.dim);
    for (int i = 0; i < dim; i++) {
      for (int j = 0; j <= i; j++) {
        double sum = 0.0d;
        for (int k = 0; k < j; k++)
          sum += L.array[i][k] * L.array[j][k];

        if (i == j)
          L.array[i][i] = Math.sqrt(this.array[i][i] - sum);
        else
          L.array[i][j] = (this.array[i][j] - sum) / L.array[j][j]; // L.array[i][j] = 1.0d /
                                                                    // L.array[j][j] *
                                                                    // (this.array[i][j] - sum);
      }
      if (L.array[i][i] <= 0.0d)
        throw new RuntimeException("MEF|Matrix is not positive definite!");
    }
    return L;
  }

  /**
   * Verifies if two matrices \f$ m_1 \f$ and \f$ m_2 \f$ are similar.
   * 
   * @param m1 matrix \f$ m_1 \f$
   * @param m2 matrix \f$ m_2 \f$
   * @return true if \f$ m_1 = m_2 \f$, false otherwise
   */
  public static boolean equals(PMatrix m1, PMatrix m2) {
    for (int i = 0; i < m1.dim; i++) {
      if (!Arrays.equals(m1.array[i], m2.array[i])) {
        return false;
      }
    }
    return true;
  }

  /**
   * Method toString.
   * 
   * @return value of the matrix as a string
   */
  public String toString() {
    String output = "";
    for (int i = 0; i < this.dim; i++) {
      output += "| ";
      for (int j = 0; j < this.dim; j++)
        output += String.format(Locale.ENGLISH, "%13.6f ", array[i][j]);
      output += "|\n";
    }
    return output;// += "]";
  }

  /**
   * Creates and returns a copy of the instance.
   * 
   * @return a clone of the instance.
   */
  public Parameter clone() {
    PMatrix param = new PMatrix(this.dim);
    param.type = this.type;
    param.array = this.array.clone();
    return param;
  }

  /**
   * Returns matrix's dimension.
   * 
   * @return matrix's dimension.
   */
  public int getDimension() {
    return this.dim;
  }

}
