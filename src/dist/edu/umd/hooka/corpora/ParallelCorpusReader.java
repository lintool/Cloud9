package edu.umd.hooka.corpora;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.util.Random;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import edu.umd.hooka.alignment.aer.ReferenceAlignment;
import edu.umd.hooka.corpora.Chunk;
import edu.umd.hooka.corpora.Language;
import edu.umd.hooka.corpora.LanguagePair;
import edu.umd.hooka.corpora.ParallelChunk;

public class ParallelCorpusReader extends DefaultHandler {

	public interface PChunkCallback {
		void handlePChunk(ParallelChunk p);
	}
	
	static class ChunkSetCB implements PChunkCallback {

		ChunkSetCB(ParallelCorpusReader pcr) { pcr_ = pcr; }
		ParallelCorpusReader pcr_;
		public void handlePChunk(ParallelChunk p) {
			pcr_.resultChunk = p;
		}
	}
	
	private ParallelChunk resultChunk = null;
	
	public ParallelCorpusReader() {
		cb_ = new ChunkSetCB(this);
		try {
		  sp = SAXParserFactory.newInstance().newSAXParser();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Couldn't build XML parser");
		}
	}
			
	PChunkCallback cb_;

	private ParallelCorpusReader(PChunkCallback cb) {
		cb_ = cb;
		try {
		  sp = SAXParserFactory.newInstance().newSAXParser();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed " + e);
		}
	}
	
	SAXParser sp = null;
		
	public ParallelChunk parseString(String xml) {
		resultChunk = null;
		try {
			sp.parse(new InputSource(new StringReader(xml)), this);
		}catch(final SAXException se) {
			resultChunk = null;
			se.printStackTrace();
			throw new RuntimeException("SaxE: " + se+"\n"+xml);
		}catch (final IOException ie) {
			resultChunk = null;
			ie.printStackTrace();
			throw new RuntimeException("ioe: " + ie);
		}
		return resultChunk;
	}
	
	public static void parseXMLDocument(String file, PChunkCallback cb) {

		//get a factory
		ParallelCorpusReader pcr = new ParallelCorpusReader(cb);
		final SAXParserFactory spf = SAXParserFactory.newInstance();
		try {

			//get a new instance of parser
			final SAXParser sp = spf.newSAXParser();
	   	 
			//parse the file and also register this class for call backs
			sp.parse(file, pcr);

		}catch(final SAXException se) {
			se.printStackTrace();
		}catch(final ParserConfigurationException pce) {
			pce.printStackTrace();
		}catch (final IOException ie) {
			ie.printStackTrace();
		}
	}
	
	ParallelChunk pchunk = null;
	
	//Event Handlers
	public void startElement(String uri, String localName, String qName,
		org.xml.sax.Attributes attributes) throws SAXException {
		//reset
		if(qName.equalsIgnoreCase("pchunk")) {
			pchunk = new ParallelChunk();
			pchunk.setName(attributes.getValue("name"));
		} else if (qName.equalsIgnoreCase("s")) {
			lang = Language.languageForISO639_1(attributes.getValue("lang"));
			tempVal = new StringBuffer();
		} else if (qName.equalsIgnoreCase("wordalignment")) {
			tempVal = new StringBuffer();
			langpair = LanguagePair.languageForISO639_1Pair(attributes.getValue("langpair"));
		} else if (qName.equalsIgnoreCase("pdoc")) {
			docName = attributes.getValue("name");
		} else {
			throw new SAXException("Unknown tag: " + qName);
		}
	}

	Language lang;
	LanguagePair langpair;
	StringBuffer tempVal;
	String docName;
	int pchunkCount = 0;
	int chunkCount = 0;
	int refAlignCount = 0;

	public void characters(char[] ch, int start, int length) throws SAXException {
		if (tempVal != null) tempVal.append(ch,start,length);
	}

	public void endElement(String uri, String localName,
		String qName) throws SAXException {

		if(qName.equalsIgnoreCase("pchunk")) {
			pchunkCount++;
			cb_.handlePChunk(pchunk);
		}else if (qName.equalsIgnoreCase("s")) {
			String s = tempVal.toString().trim();
			if (s.length() == 0) {
				System.err.println(pchunk.getName() + ": Empty segment for lang=" + lang);
			} else {
				Chunk c = new Chunk(tempVal.toString().trim());
				pchunk.addChunk(lang, c);
				chunkCount++;
				tempVal = null;
			}
		}else if (qName.equalsIgnoreCase("wordalignment")) {
			Chunk sc = pchunk.getChunk(langpair.getSource());
			if (sc == null)
				throw new RuntimeException("PChunk doesn't contain data for lang: " + langpair.getSource() + ".  Note: manual word alignment data must follow the chunk data.");
			Chunk tc = pchunk.getChunk(langpair.getTarget());
			if (tc == null)
				throw new RuntimeException("PChunk doesn't contain data for lang: " + langpair.getTarget() + ".  Note: manual word alignment data must follow the chunk data.");
			ReferenceAlignment r = new ReferenceAlignment(
					sc.getLength(),
					tc.getLength());
			r.addAlignmentPointsPharaoh(tempVal.toString().trim());
			pchunk.addReferenceAlignment(langpair, r);
			refAlignCount++;
			tempVal = null;
		}else if (qName.equalsIgnoreCase("pdoc")) {
			System.err.println("Finished parsing document " + docName);
			System.err.println("  pchunks: " + pchunkCount);
			System.err.println("  chunks: " + chunkCount);
			System.err.println("  ref alignments: " + refAlignCount);
		}else {
			throw new SAXException("Unknown tag: " + qName);
		}
	}
	
	private static void convertToXMLDocument(
			String label,
			String ifile1,
			String ifile2,
			String afile1_2,
			String ofile, 
			String oenc,
			String le,
			String lf,
			boolean readAlignments) {
		try {
			if (readAlignments) {
				if (afile1_2 == null || afile1_2.equals(""))
					throw new RuntimeException("I'm supposed to read alignments, but no alignment file is set!");
			} else
				if (afile1_2 != null && !afile1_2.equals(""))
					throw new RuntimeException("I'm not set to read alignments, but an alignment file is set!");
			BufferedReader r1 =
				    new BufferedReader(new InputStreamReader(new FileInputStream(ifile1), "UTF8"));
			BufferedReader r2 =
				    new BufferedReader(new InputStreamReader(new FileInputStream(ifile2), "UTF8"));
			BufferedReader r1_2 = null;
			if (readAlignments)
				r1_2=
					new BufferedReader(new InputStreamReader(new FileInputStream(afile1_2), "UTF8"));
			OutputStreamWriter w1 = new OutputStreamWriter(new FileOutputStream(ofile), oenc);
			Language de = Language.languageForISO639_1(lf);
			Language en = Language.languageForISO639_1(le);
			LanguagePair ende = null;
			if (readAlignments) ende = LanguagePair.languageForISO639_1Pair(le + "-" + lf);
			System.err.println("Reading " + en + " from: " + ifile1);
			System.err.println("Reading " + de + " from: " + ifile2);
			if (readAlignments)
			    System.err.println("Reading alignments (" + ende + ") from: " + afile1_2);
			BufferedWriter w =
				  new BufferedWriter(w1);
			w.write("<?xml version=\"1.0\" encoding=\""+ w1.getEncoding() + "\"?>");
			w.newLine();
			int x = ifile1.lastIndexOf('/');
			if (x < 0 || x >= ifile1.length()) x = 0;
			w.write("<pdoc name=\"" + ifile1.substring(x+1) + "\">");
			w.newLine();
			String e;
			int lc = 0;
			while ((e = r1.readLine()) != null) {
				lc += 1;
				String f = r2.readLine();
				if (f == null) {
					System.err.println("WARNING: " + ifile2 + " has fewer lines than " + ifile1);
					break;
				}
				String a = null;
				if (readAlignments) {
					a = r1_2.readLine();
					if (a==null)
						System.err.println(afile1_2 + " has fewer lines than corpora files -- dropping alignments for remaining sentences");
				}
				Chunk ec = new Chunk(e);
				Chunk fc = new Chunk(f);
				String name = label + lc;
				ParallelChunk p = new ParallelChunk();
				p.setName(name);
				p.addChunk(de, fc);
				p.addChunk(en, ec);
				if (a != null) {	
					ReferenceAlignment ra = new ReferenceAlignment(ec.getLength(), fc.getLength());
					try {
						ra.addAlignmentPointsPharaoh(a); 
						p.addReferenceAlignment(ende, ra);
					} catch (RuntimeException re) {
						System.err.println("Couldn't set alignment points for sentence # " + lc);  
						System.err.println(" " + en +": len=" + ec.getLength() + " words=" + ec);
						System.err.println(" " + de +": len=" + fc.getLength() + " words=" + fc);
						System.err.println(" " + ende + ": " + a);
					}
				}	
				w.write(p.toXML());
			}
			String t = r2.readLine();
			if (t != null)
				System.err.println("WARNING: " + ifile2 + " has more lines than " + ifile1);
			w.write("</pdoc>");
			System.out.println("Converted " + lc + " sentences");
			w.newLine();
			w.close();
			r1.close();
			r2.close();
			if (readAlignments)
				r1_2.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (false)
			try {
			parseXMLDocument("/Users/redpony/bitexts/hansards.fr-en/hansards.fr-en.xml",
				new PChunkCallback() {
					Random r = new Random(1);
					BufferedWriter br = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/tmp/bar.xml"), "UTF8"));
					public void handlePChunk(ParallelChunk p) {
						Language fr = Language.languageForISO639_1("fr");
						Language en = Language.languageForISO639_1("en");
						Chunk f = p.getChunk(fr);
						if (f == null) return;
						Chunk e = p.getChunk(en);
						if (e == null) return;
						float elen = e.getLength();
						float flen = f.getLength();
						if (elen > 40) return;
						if (flen > 40) return;
						float ra = elen / flen;
						if (ra > 1.3) return;
						try {
						  if (r.nextDouble() > 0.15) return;
						  br.write(p.toXML()); } catch (Exception e1) { e1.printStackTrace(); }
					}
					@Override
					public void finalize() {
						try { br.close(); } catch (Exception e){}
					}
			});
			} catch (Exception e) { e.printStackTrace(); }
		if (true)
			convertToXMLDocument(
					"koen_jhu_",
				"/Users/redpony/bitexts/kkn-eng-alignments/kkn.utf8",
				"/Users/redpony/bitexts/kkn-eng-alignments/eng",
				"/Users/redpony/bitexts/kkn-eng-alignments/align",
				"/tmp/foo.xml", "utf8", "ko", "en", true);
		if (false)
			convertToXMLDocument(
				"eu+nc_",
				"/Users/redpony/bitexts/corpus.en",
				"/Users/redpony/bitexts/corpus.de",
				"",
				"/tmp/foo.xml", "utf8", "en", "de", false);
	}


}
