package edu.umd.hooka;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

public class PhraseReader extends BufferedReader {
	Vocab v;
	int lang;
	
	public PhraseReader(Reader r, Vocab v, int lang) {
		super(r);
		this.v = v;
		this.lang = lang;
	}
	
	public Phrase readPhrase() throws IOException {
		String line = super.readLine();
		if (line == null) return null;
		return Phrase.fromString(lang, line, v);
	}

}
