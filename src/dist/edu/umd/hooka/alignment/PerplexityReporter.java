package edu.umd.hooka.alignment;

public class PerplexityReporter {
	double totalProb;
	int totalWords;
	
	public PerplexityReporter() {
		totalProb = 0.0;
		totalWords = 0;
	}
	
	public void addFactor(double logProb, int wordCount) {
		totalWords += wordCount;
		totalProb  -= logProb;
	}
	
	public void plusEquals(PerplexityReporter rhs) {
		totalWords += rhs.totalWords;
		totalProb  += rhs.totalProb;
	}
	
	public double getTotalLogProb() {
		return totalProb;
	}
	
	public int getTotalWordCount() {
		return totalWords;
	}
	
	public double getCrossEntropy() {
		return totalProb / (double)totalWords;
	}
	
	public void reset() {
		totalProb = 0.0;
		totalWords = 0;
	}
	
	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("CROSS ENTROPY: ").append(getCrossEntropy()).append("\tPERPLEXITY: ")
		.append(Math.pow(2.0, getCrossEntropy()));
		return sb.toString();
	}
}
