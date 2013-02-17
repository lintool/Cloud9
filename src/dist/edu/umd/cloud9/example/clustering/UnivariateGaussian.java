package edu.umd.cloud9.example.clustering;

import java.util.Random;

import edu.umd.cloud9.example.clustering.Parameter.TYPE;


/**
 * @author  Vincent Garcia
 * @author  Frank Nielsen
 * @version 1.0
 *
 * @section License
 * 
 * See file LICENSE.txt
 *
 * @section Description
 * 
 * The univariate Gaussian distribution is an exponential family and, as a consequence, the probability density function is given by
 * \f[ f(x; \mathbf{\Theta}) = \exp \left( \langle t(x), \mathbf{\Theta} \rangle - F(\mathbf{\Theta}) + k(x) \right) \f]
 * where \f$ \mathbf{\Theta} \f$ are the natural parameters.
 * This class implements the different functions allowing to express a univariate Gaussian distribution as a member of an exponential family.
 * 
 * @section Parameters
 * 
 * The parameters of a given distribution are:
 *   - Source parameters \f$\mathbf{\Lambda} = ( \mu , \sigma^2 ) \in R \times R^+\f$
 *   - Natural parameters \f$\mathbf{\Theta} = ( \theta_1 , \theta_2 ) \in R \times R^-\f$
 *   - Expectation parameters \f$ \mathbf{H} = ( \eta_1 , \eta_2 ) \in R \times R^+\f$
 */
public final class UnivariateGaussian extends ExponentialFamily<PVector, PVector>{	

	
	/**
	 * Constant for serialization.
	 */
	private static final long serialVersionUID = 1L;

	
	/**
	 * Computes the log normalizer \f$ F( \mathbf{\Theta} ) \f$.
	 * @param   T  parameters \f$ \mathbf{\Theta} = ( \theta_1 , \theta_2 ) \f$
	 * @return     \f$ F(\mathbf{\Theta}) = -\frac{\theta_1^2}{4\theta_2} + \frac{1}{2} \log \left( -\frac{\pi}{\theta_2} \right) \f$
	 */
	public double F(PVector T){
		return -0.25d * T.array[0]*T.array[0]/T.array[1] + 0.5d * Math.log(-Math.PI/T.array[1]);
	}


	/**
	 * Computes \f$ \nabla F ( \mathbf{\Theta} )\f$.
	 * @param   T  natural parameters \f$ \mathbf{\Theta} = ( \theta_1 , \theta_2 ) \f$
	 * @return     \f$ \nabla F(\mathbf{\Theta}) = \left( -\frac{\theta_1}{2 \theta_2}  , -\frac{1}{2 \theta_2} + \frac{\theta_1^2}{4 \theta_2^2} \right) \f$
	 */
	public PVector gradF(PVector T){
		PVector gradient  = new PVector(2);
		gradient.array[0] = -0.5d * T.array[0]/T.array[1];
		gradient.array[1] = 0.25d * (T.array[0]*T.array[0])/(T.array[1]*T.array[1]) - 0.5d/T.array[1];
		gradient.type     = TYPE.EXPECTATION_PARAMETER;
		return gradient;
	}


	/**
	 * Computes \f$ G(\mathbf{H})\f$.
	 * @param   H  expectation parameters \f$ \mathbf{H} = ( \eta_1 , \eta_2 ) \f$
	 * @return     \f$ G(\mathbf{H}) = - \frac{1}{2} \log ( \eta_1^2 - \eta_2 ) \f$
	 */
	public double G(PVector H){
		return -0.5d * Math.log(Math.abs(H.array[0]*H.array[0] - H.array[1]));
	}

	
	/**
	 * Computes \f$ \nabla G (\mathbf{H})\f$.
	 * @param   H  expectation parameters \f$ \mathbf{H} = ( \eta_1 , \eta_2) \f$
	 * @return     \f$ \nabla G(\mathbf{H}) = \left( -\frac{\eta_1}{\eta_1^2-\eta_2} , \frac{1}{2 (\eta_1^2-\eta_2)} \right) \f$
	 */
	public PVector gradG(PVector H){
		PVector gradient  = new PVector(2);
		double tmp        = H.array[0]*H.array[0] - H.array[1];
		gradient.array[0] = -H.array[0]/tmp;
		gradient.array[1] = 0.5d/tmp;
		gradient.type     = TYPE.NATURAL_PARAMETER;
		return gradient;
	}
	
	
	/**
	 * Computes the sufficient statistic \f$ t(x)\f$.
	 * @param   x  a point
	 * @return     \f$ t(x) = (x , x^2) \f$
	 */
	public PVector  t(PVector x){
		PVector t  = new PVector(2);
		t.array[0] = x.array[0];
		t.array[1] = x.array[0]*x.array[0];
		t.type     = TYPE.EXPECTATION_PARAMETER;
		return t;
	}


	/**
	 * Computes the carrier measure \f$ k(x) \f$.
	 * @param   x  a point
	 * @return     \f$ k(x) = 0 \f$
	 */
	public double k(PVector x){
		return 0.0d;	
	}


	/**
	 * Converts source parameters to natural parameters.
	 * @param   L  source parameters \f$ \mathbf{\Lambda} = ( \mu , \sigma^2 )\f$
	 * @return     natural parameters \f$ \mathbf{\Theta} =	\left( \frac{\mu}{\sigma^2} , -\frac{1}{2\sigma^2} \right) \f$
	 */
	public PVector Lambda2Theta(PVector L){
		PVector T  = new PVector(2);
		T.array[0] = L.array[0] / L.array[1];
		T.array[1] = -1.0d / (2*L.array[1]);
		T.type     = TYPE.NATURAL_PARAMETER;
		return T;
	}


	/**
	 * Converts natural parameters to source parameters.
	 * @param   T  natural parameters \f$ \mathbf{\Theta}  = ( \theta_1 , \theta_2 )\f$
	 * @return     source parameters  \f$ \mathbf{\Lambda} = \left( -\frac{\theta_1}{2 \theta_2} , -\frac{1}{2 \theta_2} \right) \f$
	 */
	public PVector Theta2Lambda(PVector T){
		PVector L  = new PVector(2);
		L.array[0] = - T.array[0] / (2 * T.array[1]);
		L.array[1] = - 1 / (2 * T.array[1]);
		L.type     = TYPE.SOURCE_PARAMETER;
		return L;	
	}


	/**
	 * Converts source parameters to expectation parameters.
	 * @param   L  source parameters \f$ \mathbf{\Lambda} = ( \mu , \sigma^2 )\f$
	 * @return     expectation parameters \f$ \mathbf{H} = \left( \mu , \sigma^2 + \mu^2 \right) \f$
	 */
	public PVector Lambda2Eta(PVector L){
		PVector H  = new PVector(2);
		H.array[0] = L.array[0];
		H.array[1] = L.array[0]*L.array[0] + L.array[1];
		H.type     = TYPE.EXPECTATION_PARAMETER;
		return H;
	}


	/**
	 * Converts expectation parameters to source parameters.
	 * @param   H  natural parameters \f$ \mathbf{H}       = ( \eta_1 , \eta_2 )\f$
	 * @return     source parameters  \f$ \mathbf{\Lambda} = \left( \eta_1 , \eta_2 - \eta_1^2 \right) \f$
	 */
	public PVector Eta2Lambda(PVector H){
		PVector L  = new PVector(2);
		L.array[0] = H.array[0];
		L.array[1] = H.array[1] - H.array[0] * H.array[0];
		L.type     = TYPE.SOURCE_PARAMETER;
		return L;	
	}

	
	/**
	 * Box-Muller transform/generator.
	 * @param mu      mean \f$ \mu \f$
	 * @param sigma   variance \f$ \sigma \f$
	 * @return        \f$ \mu + \sigma \sqrt{ -2 \log ( x ) } \cos (2 \pi x) \f$ where \f$ x \in \mathcal{U}(0,1)\f$
	 */
	public static double Rand(double mu, double sigma){
		return mu + sigma * Math.sqrt( -2.0d * Math.log(Math.random()) ) * Math.cos( 2.0d * Math.PI * Math.random() );	
	}

	
	/**
	 *  Box-Muller transform/generator
	 * @return \f$ \sqrt{ -2 \log ( x ) } \cos (2 \pi x) \f$ where \f$ x \in \mathcal{U}(0,1)\f$
	 */
	public static double Rand(){
		return Rand(0,1);
	}
	

	/**
	 * Computes the density value \f$ f(x;\mu,\sigma^2) \f$.
	 * @param  x      point
	 * @param  param  parameters (source, natural, or expectation)
	 * @return        \f$ f(x;\mu,\sigma^2) = \frac{1}{ \sqrt{2\pi \sigma^2} } \exp \left( - \frac{(x-\mu)^2}{ 2 \sigma^2} \right) \f$
	 */
	public double density(PVector x, PVector param){
		if (param.type==TYPE.SOURCE_PARAMETER)
			return Math.exp( - (x.array[0]-param.array[0])*(x.array[0]-param.array[0]) / (2.0d*param.array[1]) ) / (Math.sqrt(2.0d*Math.PI*param.array[1]));
		else if(param.type==TYPE.NATURAL_PARAMETER)
			return super.density(x, param);
		else
			return super.density(x, Eta2Theta(param));
	}


	/**
	 * Draws a point from the considered distribution.
	 * @param   L  source parameters \f$ \mathbf{\Lambda} = ( \mu , \sigma^2 )\f$
	 * @return     a point
	 */
	public PVector drawRandomPoint(PVector L) {
		
		// Mean and variance
		PVector mean      = new PVector(1);
		PVector variance  = new PVector(1);
		mean.array[0]     = L.array[0];
		variance.array[0] = L.array[1];
			
		// Draw the point
		Random  rand = new Random();
		PVector v    = new PVector(1);
		v.array[0]   = rand.nextGaussian() * Math.sqrt(variance.array[0]);
		return v.Plus(mean);
	}


	/**
	 * Computes the Kullback-Leibler divergence between two univariate Gaussian distributions.
	 * @param   LP  source parameters \f$ \mathbf{\Lambda}_P \f$
	 * @param   LQ  source parameters \f$ \mathbf{\Lambda}_Q \f$
	 * @return      \f$ D_{\mathrm{KL}}(f_P\|f_Q) = \frac{1}{2} \left(  2 \log \frac{\sigma_Q}{\sigma_P} + \frac{\sigma_P^2}{\sigma_Q^2} + \frac{(\mu_Q-\mu_P)^2}{\sigma_Q^2} -1\right) \f$
	 */
	public double KLD(PVector LP, PVector LQ) {
		double mP = LP.array[0];
		double vP = LP.array[1];
		double mQ = LQ.array[0];
		double vQ = LQ.array[1];
		return 0.5d * ( 2 * Math.log(Math.sqrt(vQ/vP)) + vP/vQ + ((mQ-mP)*(mQ-mP))/vQ - 1 );
	}


}
