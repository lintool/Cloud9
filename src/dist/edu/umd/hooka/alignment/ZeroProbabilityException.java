package edu.umd.hooka.alignment;

/**
 * Thrown when a probability of zero is encountered when
 * computing either a Viterbi probability or a posterior
 * in some model.
 * 
 * @author redpony
 *
 */
public class ZeroProbabilityException extends RuntimeException {

	private static final long serialVersionUID = 1243234735L;

	public ZeroProbabilityException(String reason) {
		super(reason);
	}
}
