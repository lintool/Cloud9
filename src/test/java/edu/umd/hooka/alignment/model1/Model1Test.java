package edu.umd.hooka.alignment.model1;

import junit.framework.TestCase;

import org.junit.Test;

import edu.umd.hooka.Phrase;
import edu.umd.hooka.PhrasePair;
import edu.umd.hooka.ttables.TTable_monolithic;


public class Model1Test extends TestCase {

	@Test
	public void testModel1() {
		int[] e1 = {-1,0,2,4};
		int[]ef = {1,2,1,2};
		TTable_monolithic tt = new TTable_monolithic(e1, ef, 4);
		tt.set(1, 1, 0.2f);
		tt.set(1, 2, 0.8f);
		tt.set(2, 1, 0.9f);
		tt.set(2, 2, 0.1f);
		tt.set(0, 1, 0.5f);
		tt.set(0, 2, 0.5f);
		Model1 m1 = new Model1(tt, false);
		int[] fw = {2, 1, 2, 2};
		//int[] fw = {2, 1, 2};
		int[] ew = {2, 1}; 
		PhrasePair pp = new PhrasePair(new Phrase(fw, 2), new Phrase(ew, 1));
		System.out.println(m1.computeAlignmentPosteriors(pp));
//		assertTrue(false);
	}

}
