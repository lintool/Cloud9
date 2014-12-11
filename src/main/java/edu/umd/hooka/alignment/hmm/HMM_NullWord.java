package edu.umd.hooka.alignment.hmm;

import edu.umd.hooka.PhrasePair;
import edu.umd.hooka.ttables.TTable;

/**
 * Represents an HMM that applies to a single sentence pair, which is
 * derived from the parameters stored in a TTable and an ATable object.
 * 
 * @author redpony
 *
 */
public class HMM_NullWord extends HMM {
	static final int MAX_LENGTH = 500;
	
	float p0;
	
	/**
	 * 
	 * @param ttable
	 * @param atable
	 * @param p0 - set to less than zero if it should be trained
	 */
	public HMM_NullWord(TTable ttable, ATable atable, double p0) {
		super(ttable, atable, true);
		this.p0 = (float)p0;
	}
	
	@Override
	public void buildHMMTables(PhrasePair pp) {
		int[] es = pp.getE().getWords();
		int[] fs = pp.getF().getWords();
		l = es.length;
		m = fs.length;
		emission.resize(m + 1, l + l + 1);
		e_coords.resize(m + 1, l + l + 1);
		e_coords.fill(-1);
		e_words.resize(m + 1, l + l + 1);
		e_words.fill(-1);
		for (int i = 1; i <= l + l; i++) {
			int ei = (i > l) ? 0 : es[i-1];
			for (int j = 1; j <= m; j++) {
				int fj = fs[j-1];
				if (i <= l)
					e_words.set(j, i, i - 1);
				e_coords.set(j, i, i > l ? 0 : i);
				emission.set(j, i, tmodel.get(ei, fj));
			}
		}
		//System.out.println("b:\n"+emission);

		transition.resize(l+l+1, l+l+1);
		transition.fill(0.0f);
		transition_coords.resize(l+l+1, l+l+1);
		transition_coords.fill(-1);
		for (int i_prev = 0; i_prev <= l+l; i_prev++) {
			for (int i = 1; i <= l+l; i++) {
				int coord = amodel.getCoord(i - i_prev, (char)l);
				if (i > l) coord = -1000;
				if (i_prev > l && i <= l) coord = amodel.getCoord(i - i_prev + l, (char)l);
				float tp = 0.0f;
				if (i_prev > l && i > l && i_prev != i) {
					coord = -2000;
				} else {
					if (i > l)
						tp = (p0 < 0.0) ? amodel.get(coord, (char)l, 0) : p0;
					else
						tp = amodel.get(coord, (char)l, 0);
					if (i_prev <= l && i > l) {
						if (i - l != i_prev) { tp = 0.0f; coord = -2000; }
					}
				}
				transition.set(i_prev, i, tp);
				transition_coords.set(i_prev,i,coord);
			}
		}
		
		//System.out.println("a:\n"+transition);
		//throw new RuntimeException("foo");
		//System.out.println("b:\n"+transition_coords);
	}
	
}
