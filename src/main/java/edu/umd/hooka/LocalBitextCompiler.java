package edu.umd.hooka;
import java.io.*;



public class LocalBitextCompiler {
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//Path pf = new Path(args[0]);
		//Path pe = new Path(args[1]);
		//String outputBase = args[3];
		if (args.length != 2) {
			System.err.println("Usage: " + LocalBitextCompiler.class.getName() + " <inbitext> <output.base>");
			System.exit(1);
		}

		String inf = args[0];
		String out = args[1];
		try {
			BufferedReader in = new BufferedReader(
					new InputStreamReader(new FileInputStream(inf), "UTF8"));
			DataOutputStream outf = new DataOutputStream(new BufferedOutputStream(
					new FileOutputStream(out)));

			VocabularyWritable vocE = new VocabularyWritable();
			VocabularyWritable vocF = new VocabularyWritable();
		
			String es;
			int lc = 0;
			while ((es = in.readLine()) != null) {
				lc++;
				if (lc % 1000 == 0) { System.err.print('.'); }
				if (lc % 50000 == 0) { System.err.println("[" + lc + "]"); System.gc(); }
				String[] fields = es.split("\\s*\\|\\|\\|\\s*");
				try {
					Phrase e=Phrase.fromString(0, fields[0], vocE);
					Phrase f=Phrase.fromString(1, fields[1], vocF);
					Alignment a = new Alignment(f.size(), e.size(),
								fields[2]);
					PhrasePair alignedSentence = new PhrasePair(f,e,a);
					outf.writeInt(lc);
					alignedSentence.write(outf);
				} catch (Exception e) {
					System.err.println("\nAt line "+lc+" caught: "+e);
				}
			}
			outf.writeInt(-1);
			vocE.write(outf);
			vocF.write(outf);
			outf.close();
			System.err.println("\n  Sentences: " + lc);
			System.err.println("  E-voc: " + vocE.size() + " types");
			System.err.println("  F-voc: " + vocF.size() + " types");
		} catch (Exception e) {
			System.err.println(e);
			e.printStackTrace();
		}
	}

}
