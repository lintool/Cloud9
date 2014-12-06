package edu.umd.hooka;

public interface Vocab {
	public int addOrGet(String word);
	public int get(String word);
	public String get(int index);
	public int size();
	
	public static final int MAX_VOCAB_INDEX = 1000000;
}
