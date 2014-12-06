package edu.umd.hooka.alignment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import edu.umd.hooka.corpora.Language;

public class HadoopAlignConfig extends Configuration {
	static final String KEY_MODEL1ITERATIONS = "ha.model1.iterations";
	static final String KEY_HMMITERATIONS = "ha.hmm.iterations";
	static final String KEY_HMMP0 = "ha.hmm.p0";
	static final String KEY_USEVB = "ha.vb.use";
	static final String KEY_USETRUNC = "ha.trunc.use";
	static final String KEY_USENULLWORD = "ha.use.nullword";
	static final String KEY_ALPHA = "ha.vb.alpha";
	static final String KEY_BITEXTS = "ha.inbitext";
	static final String KEY_F = "ha.sourcelang";
	static final String KEY_E = "ha.targetlang";
	static final String KEY_TTABLE = "ha.ttable.path";
	static final String KEY_ATABLE = "ha.atable.path";
	static final String KEY_EVOC = "ha.evoc";
	static final String KEY_FVOC = "ha.fvoc";
	static final String KEY_MAX_SENTLEN = "ha.max.sentlen";
	static final String KEY_HOMOGENEOUS_HMM = "ha.hmm.homogeneous";

	public HadoopAlignConfig() {}
	public HadoopAlignConfig(Configuration conf) {
		super(conf);
	}
	public HadoopAlignConfig(String root, String e, String f, String bitexts, int model1Iters, int hmmIters, boolean useNull, boolean useVB, boolean useTruncate, float alpha) {
		this.setRoot(root);
		this.setE(Language.languageForISO639_1(e));
		this.setF(Language.languageForISO639_1(f));
		this.setBitexts(bitexts);
		this.setModel1Iterations(model1Iters);
		this.setHMMIterations(hmmIters);
		this.setIncludeNullWord(useNull);
		this.setUseVariationalBayes(useVB);
		this.setUseTruncate(useTruncate);
		this.setAlpha(alpha);
		this.setMaxSentLen(200);
	}
	
	private void setRoot(String root) {
		this.set("root", root);
	}
	
	String getRoot() {
		return this.get("root", null);
	}
	
	public int getMaxSentLen() {
		return Integer.parseInt(get(KEY_MAX_SENTLEN));
	}
	public Language getE() { return Language.languageForISO639_1(get(KEY_E)); }
	public Language getF() { return Language.languageForISO639_1(get(KEY_F)); }
	public int getModel1Iterations() { return this.getInt(KEY_MODEL1ITERATIONS, 0); }
	public int getHMMIterations() { return this.getInt(KEY_HMMITERATIONS, 0); }
	public double getHMMp0() { 
		String v = this.get(KEY_HMMP0);
		if (v == null || v.equals("")) return -1.0;
		return Double.parseDouble(v);
	}
	public boolean isHMMHomogeneous() { return this.getBoolean(KEY_HOMOGENEOUS_HMM, true); }
	public boolean useVariationalBayes() { return this.getBoolean(KEY_USEVB, false); }
	public boolean includeNullWord() { return this.getBoolean(KEY_USENULLWORD, false); }
	public float getAlpha() { return this.getFloat(KEY_ALPHA, 0.0f); }
	public String getBitexts() { return this.get(KEY_BITEXTS); }
	public Path getTestBitextPath() { return null; }
	public boolean hasTestBitext() {
		return false;
	}
	public Path getTestRefPath() { return null; }
	public boolean hasTestRef() {
		return false;
	}
	public Path getTestAlignmentsPath() { return null; }
	public Path getTTablePath() {
		String tp = this.get(KEY_TTABLE);
		if (tp == null || tp.equals("")) tp = "tmp.ttable";
		return new Path(getRoot()+"/"+tp);
	}
	public Path getATablePath() {
		String tp = this.get(KEY_ATABLE);
		if (tp == null || tp.equals("")) tp = "tmp.atable";
		return new Path(getRoot()+"/"+tp);
	}
	public Path getFVocPath() {
		String tp = this.get(KEY_FVOC);
		if (tp == null || tp.equals("")) return null;
		return new Path(getRoot()+"/"+tp);
	}
	public Path getEVocPath() {
		String tp = this.get(KEY_EVOC);
		if (tp == null || tp.equals("")) return null;
		return new Path(getRoot()+"/"+tp);
	}

	public void setMaxSentLen(int n) { this.setInt(KEY_MAX_SENTLEN, n); }
	public void setModel1Iterations(int n) { this.setInt(KEY_MODEL1ITERATIONS, n); }
	public void setHMMIterations(int n) { this.setInt(KEY_HMMITERATIONS, n); }
	public void setUseVariationalBayes(boolean vb) { this.setBoolean(KEY_USEVB, vb); }
	public void setUseTruncate(boolean trunc) { this.setBoolean(KEY_USETRUNC, trunc); }
	public void setIncludeNullWord(boolean nw) { this.setBoolean(KEY_USENULLWORD, nw); }
	public void setAlpha(float alpha) { this.set(KEY_ALPHA, Float.toString(alpha)); }
	public void setBitexts(String value) { this.set(KEY_BITEXTS, value); }
	public void setE(Language e) { this.set(KEY_E, e.code()); }
	public void setF(Language f) { this.set(KEY_F, f.code()); }
	public void setTestBitextPath(Path p) {  }
	public void setTestAlignmentsPath(Path p) {  }
	public void setTestReferencePath(Path p) {  }
	public void setTTablePath(Path p) { this.set(KEY_TTABLE, p.toString()); }
	public void setATablePath(Path p) { this.set(KEY_ATABLE, p.toString()); }
	public void setEVocFile(Path p) { this.set(KEY_EVOC, p.toString()); }
	public void setFVocFile(Path p) { this.set(KEY_FVOC, p.toString()); }
	public void setHMMp0(double p0) { this.set(KEY_HMMP0, Double.toString(p0)); }
	public void setHMMHomogeneous(boolean x) { this.setBoolean(KEY_HOMOGENEOUS_HMM, x); }

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("Alignment Configuration Parameters\n")
		  .append("         E-language: ").append(getE().toString()).append('\n')
		  .append("         F-language: ").append(getF().toString()).append('\n')
		  .append("            Corpora: ").append(getBitexts()).append('\n')
		  .append("  Model1 iterations: ").append(getModel1Iterations()).append('\n')
		  .append("     HMM iterations: ").append(getHMMIterations()).append('\n')
		  .append("      Include NULL?: ").append(includeNullWord()).append('\n')
		  .append("           Training: ").append(useVariationalBayes() ? "VB" : "EM").append('\n')
		  .append("              alpha: ").append(getAlpha()).append('\n');
		return sb.toString();
	}
}
