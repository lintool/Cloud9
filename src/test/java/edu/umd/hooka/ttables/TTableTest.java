package edu.umd.hooka.ttables;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.umd.hooka.Vocab;
import edu.umd.hooka.alignment.HadoopAlign;

public class TTableTest extends TestCase {

	TTable_monolithic tt;
	Vocab vf;
	Vocab ve;
	
	public TTableTest(String name) {
		super(name);
		int[] e = {-1,0,3,5};
		int[]ef = {1,2,4,1,2};
		tt = new TTable_monolithic(e, ef, 4);
	}

	public void testGet() {
		Configuration conf = new Configuration();
		Vocab vocabEn_e2f = null;
    Vocab vocabDe_e2f = null;
    Vocab vocabEn_f2e = null;
    Vocab vocabDe_f2e = null;
    TTable_monolithic_IFAs ttable_de2en = null;
    TTable_monolithic_IFAs ttable_en2de = null;
    try {
      vocabEn_e2f = HadoopAlign.loadVocab(new Path("src/test/resources/vocab.en-de.en"), conf);
      vocabDe_e2f = HadoopAlign.loadVocab(new Path("src/test/resources/vocab.en-de.de"), conf);
      vocabEn_f2e = HadoopAlign.loadVocab(new Path("src/test/resources/vocab.de-en.en"), conf);
      vocabDe_f2e = HadoopAlign.loadVocab(new Path("src/test/resources/vocab.de-en.de"), conf);
      ttable_de2en = new TTable_monolithic_IFAs(FileSystem.get(conf), new Path("src/test/resources/ttable.de-en"), true);
      ttable_en2de = new TTable_monolithic_IFAs(FileSystem.get(conf), new Path("src/test/resources/ttable.en-de"), true);
    } catch (IOException e) {
      e.printStackTrace();
    }

		int e1 = vocabEn_e2f.get("book");
		int f1 = vocabDe_e2f.get("buch");
		System.out.println(vocabDe_e2f.get(f1)+"="+ttable_en2de.get(e1, f1));
		System.out.println(vocabEn_f2e.get(e1)+"="+ttable_de2en.get(f1, e1));

		int[] arr1 = ttable_en2de.get(e1).getTranslations(0.01f);
		for(int f : arr1){
			System.out.println(vocabDe_e2f.get(f)+"="+ttable_en2de.get(e1, f));
		}
		
		e1 = vocabEn_f2e.get("book");
		f1 = vocabDe_f2e.get("buch");
		System.out.println(vocabDe_f2e.get(f1)+"="+ttable_de2en.get(f1, e1));
		System.out.println(vocabDe_f2e.get(f1)+"="+ttable_de2en.get(f1, e1));
		
		System.out.println(ttable_de2en.getMaxE() == vocabDe_f2e.size()-1);
		System.out.println(ttable_en2de.getMaxE() == vocabEn_e2f.size()-1);
	}
	
	public void testAdd() {
		tt.clear();
		tt.add(1, 1, 0.1f);
		tt.add(1, 2, 0.1f);
		tt.add(1, 4, 0.1f);
		tt.add(1, 4, 0.2f);
		float f = tt.get(1, 4);
		assertEquals(0.3f, f);
	}

	public void testNormalize() {
		tt.clear();
		tt.add(1, 1, 0.1f);
		tt.add(1, 2, 0.2f);
		tt.add(1, 4, 0.1f);
		tt.add(2, 1, 0.2f);
		tt.normalize();
		assertEquals(0.25f, tt.get(1,1));
		assertEquals(0.5f, tt.get(1,2));
		assertEquals(1.0f, tt.get(2,1));
	}
	
	public void testTTIFA() {
//		RawLocalFileSystem rfs = new RawLocalFileSystem();
////		rfs.initialize(null, new org.apache.hadoop.conf.Configuration());
//		try {
//			TTable t = new TTable_monolithic_IFAs(rfs, new Path("/tmp/ttt_tfs.dat"), false);
//			t.set(2, new IndexedFloatArray(3));
//			t.set(1, new IndexedFloatArray(2));
//			t.set(0, new IndexedFloatArray(10));
//			t.add(1, 1, 0.1f);
//			t.add(1, 1, 0.3f);
//			t.add(0, 5, 0.5f);
//			t.normalize();
//			t.write();
//			System.out.println(t);
//			TTable s = new TTable_monolithic_IFAs(rfs, new Path("/tmp/ttt_tfs.dat"), true);
//			System.out.println("new:\n"+s);
//			assertEquals(t.get(1,1),s.get(1,1));
//			assertEquals(t.get(2,1),s.get(2,1));
//			
//			TTable b = new TTable_monolithic_IFAs(rfs, new Path("/tmp/ttx"), false);
//			b.set(5, new IndexedFloatArray(3));
//			b.set(5, 0, 0.5f);
//			System.out.println("B:"+b);
//		} catch (IOException e) {
//			e.printStackTrace();
//			fail("Caught " + e);
//		}
		
		
	}
	
	public void testGetCoord() {
		int[] e1 = {-1,0,3,6};
		int[]ef = {1,2,3,1,2,3};
		TTable_monolithic tt = new TTable_monolithic(e1, ef, 4);
		tt.set(1, 1, 0.1f);
		tt.set(1, 2, 0.7f);
		tt.set(1, 3, 0.2f);
		tt.set(2, 1, 0.5f);
		tt.set(2, 2, 0.4f);
		tt.set(2, 3, 0.1f);
	}

	
	public void testSet() {
		tt.clear();
		tt.set(1, 1, 0.8f);
		assertEquals(0.8f, tt.get(1, 1));
		tt.set(1, 1, 0.2f);
		assertEquals(0.2f, tt.get(1, 1));
	}

	public void testReadFields() {
		try {
			File temp = File.createTempFile("ttable", null);
			temp.deleteOnExit();

			DataOutputStream dos = new DataOutputStream(
				new FileOutputStream(temp));
			tt.clear();
			tt.set(1, 1, 0.08f);
			tt.set(2, 1, 0.4f);
			tt.set(2, 2, 0.3f);
			tt.set(0, 1, 0.04f);
			tt.set(0, 2, 0.25f);
			assertEquals(0.08f, tt.get(1,1));
			System.err.println(tt);
			tt.write(dos);
			dos.close();
			System.err.println("Size of tt on disk: " + dos.size());
			DataInputStream dis = new DataInputStream(
					new FileInputStream(temp));
			TTable_monolithic tt2 = new TTable_monolithic();
			tt2.readFields(dis);
			System.err.println(tt2);
			assertEquals(0.04f, tt.get(0, 1));
			assertEquals(0.08f, tt2.get(1, 1));
			dis.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Caught "+e);
		}
	}

	public void testWriteAsIndexedFloatArray() {
		try {
			File temp = File.createTempFile("ifa", null);
			temp.deleteOnExit();

			DataOutputStream dos = new DataOutputStream(
				new FileOutputStream(temp));
			tt.clear();
			tt.set(1, 1, 0.08f);
			tt.set(2, 1, 0.4f);
			tt.set(2, 2, 0.3f);
			tt.set(0, 1, 0.04f);
			assertEquals(0.08f, tt.get(1,1));
			dos.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail("Caught "+e);
		}
	}
	
	public void testSliced() {
//		RawLocalFileSystem rfs = new RawLocalFileSystem();
////		rfs.initialize(null, new org.apache.hadoop.conf.Configuration());
//		// TODO should make this work on Windows
//		TTable tts = new TTable_sliced(3, rfs, new Path("/tmp/ttt_ts.dat"));
//		int[]f1 = {1,2,4};
//		int[]f2 = {1,2};
//		tts.set(0, new IndexedFloatArray(5));
//		tts.set(1, new IndexedFloatArray(f1));
//		tts.set(2, new IndexedFloatArray(f2));
//		tt.set(1, 1, 0.08f);
//		tt.set(2, 1, 0.4f);
//		tt.set(2, 2, 0.3f);
//		tt.set(0, 1, 0.04f);
//		try {
//			tts.write();
//			TTable tts2 = new TTable_sliced(rfs, new Path("/tmp/ttt_ts.dat"));
//			assertNotSame(tts2.toString(), tts.toString());
//			tts2.add(1, 4, 0.0f);
//			tts2.add(0, 2, 0.0f);
//			tts2.add(2, 2, 0.0f);
//			assertEquals(tts2.toString(), tts.toString());
//			tts2 = new TTable_sliced(rfs, new Path("/tmp/ttt_ts.dat"));
//			tts2.add(0, 2, 1.0f);
//			tts2.write();
//			tts2 = new TTable_sliced(rfs, new Path("/tmp/ttt_ts.dat"));
//			assertEquals(tts2.get(0,2),1.0f);
//		} catch (IOException e) {
//			e.printStackTrace();
//			fail("Caught " + e);
//		}
	}

}
