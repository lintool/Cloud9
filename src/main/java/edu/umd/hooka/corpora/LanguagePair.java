package edu.umd.hooka.corpora;

import java.util.HashMap;

public class LanguagePair {
	private Language source;
	private Language target;
	static HashMap<String, LanguagePair> lpmap = new HashMap<String, LanguagePair>();
	public static LanguagePair languageForISO639_1Pair(String s) {
		if (!s.matches("^[a-z]{2}-[a-z]{2}$"))
			throw new RuntimeException("Bad format for language pair: " + s);
		LanguagePair lp = lpmap.get(s);
		if (lp != null) return lp;
		lp = new LanguagePair(s);
		lpmap.put(s, lp);
		return lp;
	}
	public int hashCode() {
		return source.hashCode() * 31 + target.hashCode() + 11;
	}
	private LanguagePair(String s) {
		source = Language.languageForISO639_1(s.substring(0, 2));
		target = Language.languageForISO639_1(s.substring(3));
	}		
	public LanguagePair inverted() {
		return LanguagePair.languageForISO639_1Pair(
				target.code()+"-"+source.code());
	}
	
	public final boolean isRelevant(Language l) {
		return (l == source || l == target);
	}
	
	public String toString() {
		return source.code() + "-" + target.code();
	}
	
	public final Language getSource() { return source; }
	public final Language getTarget() { return target; }

}
