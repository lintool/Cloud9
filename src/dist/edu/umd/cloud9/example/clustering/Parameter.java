package edu.umd.cloud9.example.clustering;

import java.io.Serializable;


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
 * A statistical distribution is parameterized by a set of values (parameters).
 * The Parameter class implements a parameter object.
 * Parameters can be a vector (class PVector), a matrix (class PMatrix), or both (class PVectorMatrix).
 * The Parameter class is an abstract class.
 */
abstract public class Parameter implements Serializable{


	/**
	 * Constant for serialization.
	 */
	private static final long serialVersionUID = 1L;
	
	
	/**
	 * Type of the parameters: source parameters, natural parameters, or expectation parameters.
	 */
	public static enum TYPE {SOURCE_PARAMETER, NATURAL_PARAMETER, EXPECTATION_PARAMETER};
	
	
	/**
	 * Type of the parameters.
	 */
	public TYPE type;

	
	/**
	 * Class constructor. By default, a parameter object corresponds to source parameters. 
	 */
	public Parameter(){
		this.type = TYPE.SOURCE_PARAMETER;	
	}
	

	/**
	 * Adds (not in place) the current parameter p to the parameter q.
	 * @param   q  parameter
	 * @return     p+q
	 */
	abstract public Parameter Plus(Parameter q);
	

	/**
	 * Subtracts (not in place) the parameter q to the current parameter p.
	 * @param   q  parameter
	 * @return     p-q
	 */
	abstract public Parameter Minus(Parameter q);


	/**
	 * Multiplies (not in place) the current parameter p by a real number \f$ \lambda \f$.
	 * @param  lambda  value \f$ \lambda \f$
	 * @return         \f$ \lambda p \f$
	 */
	abstract public Parameter Times(double lambda);


	/**
	 * Computes the inner product (real number) between the current parameter p and the parameter q.
	 * @param   q  parameter
	 * @return     \f$ \langle p , q \rangle \f$ 
	 */
	abstract public double InnerProduct(Parameter q);
	
	
	/**
	 * Creates and returns a copy of the instance.
	 * @return a clone of the instance.
	 */
	abstract public Parameter clone();
	
	
	/**
	 * Returns the dimension of the parameters.
	 * @return parameters' dimension.
	 */
	abstract public int getDimension();

}
