package edu.umd.hooka.alignment;

public class Digamma {

	/**
	 * Stolen from from http://www.cog.brown.edu/~mj/Software.htm
	 * @param x
	 * @return digamma(x)
	 */
	public static double digamma(double x) {
		double result = 0.0;
		assert(x > 0.0);
		for ( ; x < 7.0; ++x)
			result -= 1.0/x;
		x -= 1.0/2.0;
		double xx = 1.0/x;
		double xx2 = xx*xx;
		double xx4 = xx2*xx2;
		result += Math.log(x)+(1.0/24.0)*xx2-(7.0/960.0)*xx4+(31.0/8064.0)*xx4*xx2-(127.0/30720.0)*xx4*xx4;
		return result;
	}
}
