package edu.umd.cloud9.example.clustering;

import java.io.Serializable;

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
 *          This class integrates the Kullback-Leibler divergence and conversion procedures inside
 *          the exponential family. - ParamD are the distribution source parameters, its dimension
 *          is the order of the exponential family. - ParamX are the type of observations.
 */
abstract public class ExponentialFamily<ParamX extends Parameter, ParamD extends Parameter>
    implements Serializable {

  /**
   * Constant for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * Computes the log normalizer \f$ F( \mathbf{\Theta} ) \f$.
   * 
   * @param T natural parameters \f$ \mathbf{\Theta}\f$
   * @return \f$ F(\mathbf{\Theta}) \f$
   */
  abstract public double F(ParamD T);

  /**
   * Computes \f$ \nabla F ( \mathbf{\Theta} )\f$.
   * 
   * @param T expectation parameters \f$ \mathbf{\Theta} \f$
   * @return \f$ \nabla F( \mathbf{\Theta} ) \f$
   */
  abstract public ParamD gradF(ParamD T);

  /**
   * Computes \f$ div F \f$.
   * 
   * @param TP natural parameters \f$ \mathbf{\Theta}_P\f$
   * @param TQ natural parameters \f$ \mathbf{\Theta}_Q\f$
   * @return \f$ div F( \mathbf{\Theta}_P \| \mathbf{\Theta}_Q ) = F(\mathbf{\Theta}_P) -
   *         F(\mathbf{\Theta}_Q) - \langle \mathbf{\Theta}_P-\mathbf{\Theta}_Q , \nabla
   *         F(\mathbf{\Theta}_Q) \rangle\f$
   */
  public double DivergenceF(ParamD TP, ParamD TQ) {
    return F(TP) - F(TQ) - (TP.Minus(TQ)).InnerProduct(gradF(TQ));
  }

  /**
   * Computes \f$ G(\mathbf{H})\f$
   * 
   * @param H expectation parameters \f$ \mathbf{H} \f$
   * @return \f$ G(\mathbf{H}) \f$
   */
  abstract public double G(ParamD H);

  /**
   * Computes \f$ \nabla G (\mathbf{H})\f$
   * 
   * @param H expectation parameters \f$ \mathbf{H} \f$
   * @return \f$ \nabla G(\mathbf{H}) \f$
   */
  abstract public ParamD gradG(ParamD H);

  /**
   * Computes \f$ div G \f$.
   * 
   * @param HP expectation parameters \f$ \mathbf{H}_P\f$
   * @param HQ expectation parameters \f$ \mathbf{H}_Q\f$
   * @return \f$ div G( \mathbf{H}_P \| \mathbf{H}_Q ) = G(\mathbf{H}_P) - G(\mathbf{H}_Q) - \langle
   *         \mathbf{H}_P-\mathbf{H}_Q , \nabla G(\mathbf{H}_Q) \rangle\f$
   */
  public double DivergenceG(ParamD HP, ParamD HQ) {
    return G(HP) - G(HQ) - (HP.Minus(HQ)).InnerProduct(gradG(HQ));
  }

  /**
   * Computes the sufficient statistic \f$ t(x)\f$.
   * 
   * @param x a point
   * @return \f$ t(x) \f$
   */
  abstract public ParamD t(ParamX x);

  /**
   * Computes the carrier measure \f$ k(x) \f$.
   * 
   * @param x a point
   * @return \f$ k(x) \f$
   */
  abstract public double k(ParamX x);

  /**
   * Converts source parameters to natural parameters.
   * 
   * @param L source parameters \f$ \mathbf{\Lambda} \f$
   * @return natural parameters \f$ \mathbf{\Theta} \f$
   */
  public abstract ParamD Lambda2Theta(ParamD L);

  /**
   * Converts natural parameters to source parameters.
   * 
   * @param T natural parameters \f$ \mathbf{\Theta}\f$
   * @return source parameters \f$ \mathbf{\Lambda} \f$
   */
  public abstract ParamD Theta2Lambda(ParamD T);

  /**
   * Converts source parameters to expectation parameters.
   * 
   * @param L source parameters \f$ \mathbf{\Lambda} \f$
   * @return expected parameters \f$ \mathbf{H} \f$
   */
  public abstract ParamD Lambda2Eta(ParamD L);

  /**
   * Converts expectation parameters to source parameters.
   * 
   * @param H expectation parameters \f$ \mathbf{H} \f$
   * @return source parameters \f$ \mathbf{\Lambda} \f$
   */
  public abstract ParamD Eta2Lambda(ParamD H);

  /**
   * Converts natural parameters to expectation parameters.
   * 
   * @param T natural parameters \f$ \mathbf{\Theta}\f$
   * @return expectation parameters \f$ \mathbf{H} \f$
   */
  public ParamD Theta2Eta(ParamD T) {
    return gradF(T);
  }

  /**
   * Converts expectation parameters to natural parameters.
   * 
   * @param H expectation parameters \f$ \mathbf{H} \f$
   * @return natural parameters \f$ \mathbf{\Theta} \f$
   */
  public ParamD Eta2Theta(ParamD H) {
    return gradG(H);
  }

  /**
   * Computes the density value \f$ f(x;\mathbf{\Theta}) \f$ of an exponential family member.
   * 
   * @param x a point
   * @param T natural parameters \f$ \mathbf{\Theta} \f$
   * @return \f$ f(x) = \exp \left( \langle \mathbf{\Theta} \ , \ t(x) \rangle - F(\mathbf{\Theta})
   *         + k(x) \right) \f$
   */
  public double density(ParamX x, ParamD T) {
    return Math.exp(T.InnerProduct(t(x)) - F(T) + k(x));
  }

  /**
   * Computes the Bregman divergence between two members of a same exponential family.
   * 
   * @param T1 natural parameters \f$ \mathbf{\Theta}_1\f$
   * @param T2 natural parameters \f$ \mathbf{\Theta}_2\f$
   * @return \f$ BD( \mathbf{\Theta_1} \| \mathbf{\Theta_2} ) = F(\mathbf{\Theta_1}) -
   *         F(\mathbf{\Theta_2}) - \langle \mathbf{\Theta_1} - \mathbf{\Theta_2} , \nabla
   *         F(\mathbf{\Theta_2}) \rangle \f$
   */
  public double BD(ParamD T1, ParamD T2) {
    return F(T1) - F(T2) - gradF(T2).InnerProduct(T1.Minus(T2));
  }

  /**
   * Computes the Kullback-Leibler divergence between two members of a same exponential family.
   * 
   * @param LP source parameters \f$ \mathbf{\Lambda}_P \f$
   * @param LQ source parameters \f$ \mathbf{\Lambda}_Q \f$
   * @return \f$ D_{\mathrm{KL}}(f_P\|f_Q) \f$
   */
  public abstract double KLD(ParamD LP, ParamD LQ);

  /**
   * Computes the geodesic point.
   * 
   * @param T1 natural parameters \f$ \mathbf{\Theta}_1\f$
   * @param T2 natural parameters \f$ \mathbf{\Theta}_2\f$
   * @param alpha position \f$ \alpha \f$ of the point on the geodesic link
   * @return \f$ \nabla G \left( (1-\alpha) \nabla F (\mathbf{\Theta}_1) + \alpha \nabla F
   *         (\mathbf{\Theta}_2) \right) \f$
   */
  public ParamD GeodesicPoint(ParamD T1, ParamD T2, double alpha) {
    return gradG((ParamD) (gradF(T1).Times(1.0d - alpha)).Plus(gradF(T2).Times(alpha)));
  }

  /**
   * Draws a random point from the considered distribution.
   * 
   * @param L source parameters \f$ \mathbf{\Lambda}\f$
   * @return a point
   */
  public abstract ParamX drawRandomPoint(ParamD L);

}
