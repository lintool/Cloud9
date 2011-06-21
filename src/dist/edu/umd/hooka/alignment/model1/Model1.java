package edu.umd.hooka.alignment.model1;

import java.util.Arrays;

import org.apache.hadoop.mapred.Reporter;

import edu.umd.hooka.Alignment;
import edu.umd.hooka.AlignmentPosteriorGrid;
import edu.umd.hooka.PhrasePair;
import edu.umd.hooka.alignment.CrossEntropyCounters;
import edu.umd.hooka.alignment.PerplexityReporter;
import edu.umd.hooka.ttables.TTable;

public class Model1 extends Model1Base {

	float[] totals = null;
	protected TTable tmodel = null;
	
	public Model1(TTable ttable, boolean useNullWord) {
		super(useNullWord); // include Null word
		tmodel = ttable;
	}

	public void clearModel() {
		tmodel = null;
		totals = null;
	}

	@Override
	public void processTrainingInstance(PhrasePair pp, Reporter reporter) {
		initializeCountTableForSentencePair(pp);
		int fw[] = pp.getF().getWords();
		int ew[] = pp.getE().getWords();
		if (totals == null) {
			totals = new float[maxF];
		} else {
			Arrays.fill(totals, 0.0f);
		}
		if (_includeEnglishNullWord) {
			// handle NULL
			for (int fj:fw) {	 
				totals[fj] += tmodel.get(0, fj);
			}
		}
		// handle normal e_i's
		for (int ei:ew) {
			for (int fj:fw) {	 
				totals[fj] += tmodel.get(ei, fj);
			}
		}
		
		float totalLogProb = 0.0f;
		for (int j=0; j<fw.length; j++) {
			int fj = fw[j];
			float totalProb = totals[fj];
			totalLogProb += Math.log(totalProb);
			for (int i=0; i<=ew.length; i++) {
				int ei = (i > 0) ? ew[i-1] : 0;
				addTranslationCount(i, j, tmodel.get(ei, fj) / totalProb);
			}
		}
		if (reporter != null) {
			totalLogProb -= ((float)fw.length) * Math.log(1.0f + (float)ew.length);
			reporter.incrCounter(CrossEntropyCounters.LOGPROB, (long)(-totalLogProb));
			reporter.incrCounter(CrossEntropyCounters.WORDCOUNT, fw.length);
			reporter.progress();
		}
	}
	
	public AlignmentPosteriorGrid computeAlignmentPosteriors(PhrasePair pp) {
		AlignmentPosteriorGrid res = new AlignmentPosteriorGrid(pp);
		int fw[] = pp.getF().getWords();
		int ew[] = pp.getE().getWords();
		if (totals == null) {
			totals = new float[maxF];
		} else {
			Arrays.fill(totals, 0.0f);
		}
		if (_includeEnglishNullWord) {
			// handle NULL
			for (int fj:fw) {	 
				totals[fj] += tmodel.get(0, fj);
			}
		}
		// handle normal e_i's
		for (int ei:ew) {
			for (int fj:fw) {	 
				totals[fj] += tmodel.get(ei, fj);
			}
		}
		
		float totalLogProb = 0.0f;
		for (int j=0; j<fw.length; j++) {
			int fj = fw[j];
			float totalProb = totals[fj];
			totalLogProb += Math.log(totalProb);
			int start = 1;
			if (_includeEnglishNullWord)
				start = 0;
			for (int i=start; i<=ew.length; i++) {
				int ei = (i > 0) ? ew[i-1] : 0;
				float post = tmodel.get(ei, fj) / totalProb;
				res.setAlignmentPointPosterior(j, i, post);
			}
		}
		return res;
	}

	@Override
	public Alignment viterbiAlign(PhrasePair sentence, PerplexityReporter viterbiPerp) {
		int[] es = sentence.getE().getWords();
		int[] fs = sentence.getF().getWords(); 
		float threshold = 0.27f;
		Alignment res = new Alignment(fs.length, es.length);
		AlignmentPosteriorGrid g = computeAlignmentPosteriors(sentence);
		for (int j=0; j<fs.length; j++) {
			for (int i=1; i<es.length; i++) {
				float post = g.getAlignmentPointPosterior(j, i);
				if (post > threshold) { res.align(j, i-1); }
			}
		}
		return res;
	}

	public Alignment realViterbiAlign(PhrasePair sentence, PerplexityReporter viterbiPerp) {
		int[] es = sentence.getE().getWords();
		int[] fs = sentence.getF().getWords(); 
		Alignment res = new Alignment(fs.length, es.length);
		float viterbiScore = 0.0f;
		for (int j=0; j<fs.length; j++) {
			float bestProb = -1.0f;
			int besti = -1;
			int starti = 0; // TODO - should use NULL?
			if (_includeEnglishNullWord) starti = -1;
			for (int i=starti; i<es.length; i++) {
				float curProb = 0.0f;
				if (i == -1)
					curProb = tmodel.get(0, fs[j]);
				else
					curProb = tmodel.get(es[i], fs[j]);
				if (curProb > bestProb) { bestProb = curProb; besti = i; }
			}
			if (besti < 0) {
				; //throw new RuntimeException("Implement or ignore!");
			} else {
				res.align(j, besti);
			}
			viterbiScore += Math.log(bestProb);
		}
		viterbiPerp.addFactor(viterbiScore - (fs.length * Math.log(es.length + 1.0)), fs.length);
		return res;
	}

}
