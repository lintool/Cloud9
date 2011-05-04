package edu.umd.hooka;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.TreeSet;

import org.junit.Test;

public class WordPairTest {

	@Test
	public void testCompareTo() {
		WordPair a = new WordPair(1, 5);
		WordPair b = new WordPair(1, 2);
		WordPair c = new WordPair(2, 3);
		WordPair d = new WordPair(3, 6);
		TreeSet<WordPair> t = new TreeSet<WordPair>();
		t.add(d); t.add(b); t.add(c); t.add(a);
		Iterator<WordPair> i = t.iterator();
		assertEquals(i.next(), b);
		assertEquals(i.next(), a);
		assertEquals(i.next(), c);
		assertEquals(i.next(), d);
	}
	
	@Test
	public void testPhrase() {
		WordPair a = new WordPair(1, 5);
		WordPair b = new WordPair(1, 2);
		WordPair c = new WordPair(2, 3);
		WordPair d = new WordPair(3, 6);
		TreeSet<WordPair> v = new TreeSet<WordPair>();
		v.add(d); v.add(b); v.add(c); v.add(a);
		Phrase op = new Phrase();
		int l = -1;
		int cur = -1;
		int i = 0;
		Iterator<WordPair> it = v.iterator();
		for (WordPair p : v) {
			if (p.getE() != cur) {
				if (l != -1) {
					int len = i-l;
					int[] fs = new int[len];
					int cp = 0;
					while(cp < len) {
						fs[cp] = it.next().getF();
						cp++;
					}
					op.setWords(fs);
					System.out.println(cur + ": " + op);
				}
				l = i;
				cur = p.getE();
			}
			i++;
		}
		if (l != -1) {
			int len = i-l;
			int[] fs = new int[len];
			int cp = 0;
			while(cp < len) {
				fs[cp] = it.next().getF();
				cp++;
			}
			op.setWords(fs);
			System.out.println(cur + ": " + op);
		}
	}

}
