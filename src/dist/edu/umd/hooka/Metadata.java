package edu.umd.hooka;
import java.io.Serializable;

public class Metadata implements Serializable{
	
	private static final long serialVersionUID = -3357451092297773289L;

	public int numSentences;
	public int eVocabSize;
	public int fVocabSize;
	
	public Metadata(int n, int e, int f)
	{
		numSentences = n;
		eVocabSize = e;
		fVocabSize = f;
	}
}
