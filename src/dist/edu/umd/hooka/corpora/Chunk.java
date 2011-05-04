package edu.umd.hooka.corpora;

public final class Chunk {
	public String[] words;
	public Chunk(String chunk) {
		this.words = chunk.split("\\s+");
	}
	public String toString() {
		StringBuffer sb = new StringBuffer();
		for (int i =0; i < words.length; i++){
			if (i > 0) sb.append(' ');
			sb.append(words[i]);
		}
		return sb.toString();
	}
	public final String getWord(int i) { return words[i]; }
	public final String[] getWords() { return words; }
	public final int getLength() { return words.length; }
	public int hashCode() {
		int hc = 1;
		for (String w : words) {
			hc *= 31;
			hc += w.hashCode();
		}
		return hc;
	}
}
