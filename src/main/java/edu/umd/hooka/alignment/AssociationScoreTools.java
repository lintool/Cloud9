package edu.umd.hooka.alignment;

public class AssociationScoreTools {

	/**
	 * Computes the log-likelihood ratio 
	 * @param countEF
	 * @param countE
	 * @param countF
	 * @param N
	 */
	public static double computeLLR(int countEF, int countE, int countF, int N) {
		double cEF = countEF;
		double cE = countE;
		double cF = countF;
		double numS = N;
		double countnotE = numS - cE;
		double countnotF = numS - cF;
		double countEnotF = cE - cEF;
		double countnotEF = cF - cEF;
		double countnotEnotF = numS - (countnotEF + countEnotF + cEF);
		double pEF = 1.0;
		double pEnotF= 1.0;
		double pnotEF = 1.0;
		double pnotEnotF = 1.0;
		//Note that if the counts are zero the log will be multiplied by zero so it doesn't matter
		//Also note the pEF etc. are the RATIOS p(E|F)/p(E) etc. not the acutal probabilities

		if (countEF > 0) pEF = cEF * (numS/(cE*cF));
		if (countEnotF > 0) pEnotF = (countEnotF * numS)/(cE * countnotF);
		if (countnotEF > 0) pnotEF = (countnotEF * numS)/(countnotE * cF);
		if (countnotEnotF > 0) pnotEnotF = (countnotEnotF * numS) /(countnotE*countnotF);
		double llrScore = cEF * Math.log(pEF) + countEnotF * Math.log(pEnotF) + countnotEF * Math.log(pnotEF) + countnotEnotF * Math.log(pnotEnotF);
		if(llrScore < 0.0) llrScore = 0.0;
		return llrScore;
	}
	
	static double lgamma(double x) {
		if (x < 0.0) return lgamma(1.0);
		double tmp = (x - 0.5) * Math.log(x + 4.5) - (x + 4.5);
		double ser = 1.0 + 76.18009173    / (x + 0)   - 86.50532033    / (x + 1)
	    	+ 24.01409822    / (x + 2)   -  1.231739516   / (x + 3)
	    	+  0.00120858003 / (x + 4)   -  0.00000536382 / (x + 5);
		final double res = tmp + Math.log(ser * Math.sqrt(2 * Math.PI));
		System.err.println("lg("+x+")="+res);
		return res;
	}

	public static double fishersExact(int countEF, int countE, int countF, int N) {
	    double a = countEF;
	    double b = (countF - countEF);
	    double c = (countE - countEF);
	    double d = (N - countE - countF + countEF);
	    double n = a + b + c + d;
	    
	    double xp = lgamma(1.0+a+c) + lgamma(1.0+b+d) + lgamma(1.0+a+b) + lgamma(1.0+c+d) - lgamma(1.0+n) - lgamma(1.0+a) - lgamma(1.0+b) - lgamma(1.0+c) - lgamma(1.0+d);
	    System.err.println("xp="+xp);
	    double cp = Math.exp(lgamma(1.0+a+c) + lgamma(1.0+b+d) + lgamma(1.0+a+b) + lgamma(1.0+c+d) - lgamma(1.0+n) - lgamma(1.0+a) - lgamma(1.0+b) - lgamma(1.0+c) - lgamma(1.0+d));
	    System.err.println("cp="+cp);
	    double total_p = 0.0;
	    int tc = Math.min((int)b,(int)c);
	    for (int i=0; i<=tc; i++) {
	      total_p += cp;
	      double coef = b*c/(a+1.0)/(d+1.0);
	      cp *= coef;
	      ++a; --c; ++d; --b;
	    }
	  return total_p;

	}

	
}
