package edu.umd.hooka;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import edu.umd.hooka.corpora.Chunk;
import edu.umd.hooka.corpora.Language;
import edu.umd.hooka.corpora.LanguagePair;
import edu.umd.hooka.corpora.ParallelChunk;
import edu.umd.hooka.corpora.ParallelCorpusReader.PChunkCallback;

public class CreateWordAlignmentCorpus {

	static class WriterCallback implements PChunkCallback {
		BufferedWriter ew;
		BufferedWriter fw;
		BufferedWriter lw;
		Language ar = Language.languageForISO639_1("ar");
		Language en = Language.languageForISO639_1("en");
		LanguagePair lp = LanguagePair.languageForISO639_1Pair("ar-en");
		AlignmentWordPreprocessor sawp;
		AlignmentWordPreprocessor tawp;
		WriterCallback(String e, String f, String l) throws IOException {
			ew = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(e), "UTF8"));
			fw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), "UTF8"));
			lw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(l), "UTF8"));
			sawp = AlignmentWordPreprocessor.CreatePreprocessor(lp, ar, null);
			tawp = AlignmentWordPreprocessor.CreatePreprocessor(lp, en, null);
		}
		public void close() throws IOException {
			ew.flush();
			ew.close();
			fw.flush();
			fw.close();
		}
		static final int MAX_LENGTH = 99;
		public void handlePChunk(ParallelChunk p) {
			Chunk a = p.getChunk(ar);
			Chunk e = p.getChunk(en);
			if (a == null) return;
			if (e == null) return;
			String[] npa = a.getWords();
			String[] npe = e.getWords();
			if (npa.length > MAX_LENGTH)
				return;
			if (npe.length > MAX_LENGTH)
				return;
			if (npa.length == 0 || npe.length == 0)
				return;
			String[] aws = sawp.preprocessWordsForAlignment(npa);
			String[] ews = tawp.preprocessWordsForAlignment(npe);
			StringBuffer asb = new StringBuffer();
			for (String i : aws)
				asb.append(i).append(' ');
			asb.deleteCharAt(asb.length() - 1);
			StringBuffer esb = new StringBuffer();
			for (String i : ews)
				esb.append(i).append(' ');
			esb.deleteCharAt(esb.length() - 1);
			try {
			  lw.write(p.getName());
			  fw.write(asb.toString());
			  ew.write(esb.toString());
			  lw.newLine();
			  fw.newLine();
			  ew.newLine();
			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(1);
			}
		}
	}
	
	public static void main(String[] args) {
		if (args.length != 3) {
			System.err.println("Usage: CreateWordAlignmentCorpus <lang> <infile.txt> <outfile.txt>");
			System.err.println("          (note: lang must be a two-letter ISO639 code)");
			System.exit(1);
		}
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(args[1]), "UTF8"));
			Language fl = Language.languageForISO639_1(args[0]);
			LanguagePair lp = LanguagePair.languageForISO639_1Pair(args[0]+"-en");
			AlignmentWordPreprocessor sawp = AlignmentWordPreprocessor.CreatePreprocessor(lp, fl, null);
			String l;
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(args[2]), "UTF8"));
			while ((l =in.readLine()) != null) {
				String[] res = sawp.preprocessWordsForAlignment(l.split("\\s+"));
				boolean first = true;
				for (String r : res) {
					if (first)
						first = false;
					else
						out.write(' ');
					out.write(r);
				}
				out.newLine();
			}
			out.flush();
			out.close();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

}
