package edu.umd.hooka.alignment.hmm;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.hooka.Alignment;
import edu.umd.hooka.AlignmentPosteriorGrid;
import edu.umd.hooka.Array2D;
import edu.umd.hooka.PhrasePair;
import edu.umd.hooka.alignment.PartialCountContainer;
import edu.umd.hooka.alignment.PerplexityReporter;
import edu.umd.hooka.alignment.CrossEntropyCounters;
import edu.umd.hooka.alignment.ZeroProbabilityException;
import edu.umd.hooka.alignment.model1.Model1;
import edu.umd.hooka.ttables.TTable;

/**
 * Represents an HMM that applies to a single sentence pair, which is
 * derived from the parameters stored in a TTable and an ATable object.
 * 
 * @author redpony
 *
 */
public class HMM extends Model1 {
	public static final IntWritable ACOUNT_VOC_ID = new IntWritable(999999);
	static final int MAX_LENGTH = 500;
	static final float THRESH =0.5f;
	
	/**
	 * (s,j) = p(f_j|e(s))
	 */
	Array2D emission = new Array2D(MAX_LENGTH * MAX_LENGTH);

	/**
	 * (s,j) = i s.t. e(s) = e_i or -1 if n.a.
	 */
	IntArray2D e_coords = new IntArray2D(MAX_LENGTH * MAX_LENGTH);
	
	/**
	 * (s,j) = the english word corresponding to state s
	 */
	IntArray2D e_words = new IntArray2D(MAX_LENGTH * MAX_LENGTH);
	
	/**
	 * (i',i) = p(i-i')
	 */
	Array2D transition = new Array2D(MAX_LENGTH * MAX_LENGTH);
	IntArray2D transition_coords = new IntArray2D(MAX_LENGTH * MAX_LENGTH);

	Array2D alphas = new Array2D(MAX_LENGTH * MAX_LENGTH);
	Array2D betas  = new Array2D(MAX_LENGTH * MAX_LENGTH);

	Array2D viterbi = new Array2D(MAX_LENGTH * MAX_LENGTH);
	IntArray2D backtrace = new IntArray2D(MAX_LENGTH * MAX_LENGTH);
	
	ATable amodel;
	ATable acounts;
	
	int l = -1;
	int m = -1;
	AlignmentPosteriorGrid m1_post = null;
	
	public void setModel1Posteriors(AlignmentPosteriorGrid m1pg) {
		m1_post = m1pg;
	}

	protected HMM(TTable ttable, ATable atable, boolean useNull) {
		super(ttable, useNull);
		amodel = atable;
		acounts = (ATable)amodel.clone(); acounts.clear();		
	}
	
	public HMM(TTable ttable, ATable atable) {
		super(ttable, false);
		amodel = atable;
		acounts = (ATable)amodel.clone(); acounts.clear();
	}
	
	public void writePartialCounts(OutputCollector<IntWritable,PartialCountContainer> output) throws IOException
	{
		super.writePartialCounts(output);
		PartialCountContainer pcc = new PartialCountContainer();
		pcc.setContent(acounts);
		output.collect(ACOUNT_VOC_ID, pcc);
		acounts.clear();
	}
	
	public void buildHMMTables(PhrasePair pp) {
		int[] es = pp.getE().getWords();
		int[] fs = pp.getF().getWords();
		l = es.length;
		m = fs.length;
		emission.resize(m + 1, l + 1);
		e_coords.resize(m + 1, l + 1);
		e_words.resize(m + 1, l + 1);
		e_words.fill(-1);
		e_coords.fill(-1);
		for (int i = 1; i <= l; i++) {
			int ei = es[i-1];
			for (int j = 1; j <= m; j++) {
				int fj = fs[j-1];
				e_coords.set(j, i, i);
				emission.set(j, i, tmodel.get(ei, fj));
				e_words.set(j, i, i - 1);
			}
		}
		//System.out.println("b:\n"+emission);

		transition.resize(l+1, l+1);
		transition_coords.resize(l+1, l+1);
		transition_coords.fill(-1);
		for (int i_prev = 0; i_prev <= l; i_prev++) {
			for (int i = 1; i <= l; i++) {
				transition_coords.set(i_prev, i, amodel.getCoord(i - i_prev, (char)l));
				transition.set(i_prev, i, amodel.get(i - i_prev, (char)l));
			}
		}
		
		//System.out.println("a:\n"+transition);
	}
	
	public final int getNumStates() {
		return transition.getSize2();
	}
	
	public final float getTransitionProb(int s_prev, int s) {
		return transition.get(s_prev, s);
	}
	
	public final float getEmissionProb(int j, int s) {
		return emission.get(j, s);
	}
	
	public final void addPartialJumpCountsToATable(ATable ac) {
		ac.plusEquals(acounts);
	}

	@Override
	public void processTrainingInstance(PhrasePair pp, Reporter r) {
		if (pp.getE().size() >= amodel.getMaxDist()-1) return;
		if (pp.getF().size() >= amodel.getMaxDist()-1) return;
		if (pp.getE().size() == 0) return;
		if (pp.getF().size() == 0) return;

		this.buildHMMTables(pp);
		float totalLogProb = this.baumWelch(pp, null);
		if (r != null) {
			r.incrCounter(CrossEntropyCounters.LOGPROB, (long)(-totalLogProb));
			r.incrCounter(CrossEntropyCounters.WORDCOUNT, pp.getF().size());
		}
	}

	/**
	 * @return negative log probability of sentence
	 */
	public final float baumWelch(PhrasePair pp, AlignmentPosteriorGrid pg) {

		initializeCountTableForSentencePair(pp);

		int[] obs = pp.getF().getWords();
		int J = obs.length + 1;
		int numStates = getNumStates();
		int l = pp.getE().getWords().length;
		float[] anorms = new float[J];
		alphas.resize(J + 1, getNumStates());
		betas.resize(J + 1, getNumStates());
		alphas.set(0, 0, 1.0f); anorms[0]=1.0f;
		Alignment m1a = null;
		if (m1_post != null)
			m1a = m1_post.alignPosteriorThreshold(THRESH);
		for (int j = 1; j < J; j++) {
			//System.out.println("J="+j);
			for (int s = 0; s < numStates; s++) {
				float alpha = 0.0f;
				float m1boost = 1.0f;
				float m1penalty = 0.0f;
				boolean use_m1 = false;
				if (m1a != null && m1a.isFAligned(j-1)) {
					float m1post = 0.0f;
					use_m1 = true;
					for (int i=0; i<l; i++)
						if (m1a.aligned(j-1, i))
							m1post = m1_post.getAlignmentPointPosterior(j-1, i+1);
					//System.out.println(m1post);
					m1boost = (float)(Math.sqrt(m1post));
					m1penalty = 1.0f - m1boost;
				}
				for (int s_prev = 0; s_prev < numStates; s_prev++) {
					float trans = getTransitionProb(s_prev, s);
					if (use_m1) {
						if (s <= l && s > 0 && m1a.aligned(j-1, s-1))
							trans = m1boost;
						else
							trans *= m1penalty;
					}
					alpha += alphas.get(j - 1, s_prev) * trans;
				}
				alpha *= getEmissionProb(j, s);
				//System.out.println(" ep:" + hmm.getEmissionProb(s, j));
				alphas.set(j, s, alpha);
			}
			//anorms[j] = 1.0f;
			try {
				anorms[j] = alphas.normalizeColumn(j);
			} catch (ZeroProbabilityException ex) {
				this.notifyUnalignablePair(pp, ex.getMessage());
				return 0.0f;
			}
		}
		for (int s=1; s<numStates; s++)
			betas.set(J-1, s, 1.0f);
		for (int j=J-2; j>=1; j--) {
			//System.out.println("J="+j);
			for (int s = 0; s < numStates; s++) {
				float beta = 0.0f;
				float m1boost = 1.0f;
				float m1penalty = 0.0f;
				boolean use_m1 = false;
				if (m1a != null && m1a.isFAligned(j-1)) {
					float m1post = 0.0f;
					use_m1 = true;
					for (int i=0; i<l; i++)
						if (m1a.aligned(j-1, i))
							m1post = m1_post.getAlignmentPointPosterior(j-1, i+1);
					m1boost = (float)(Math.sqrt(m1post));
					m1penalty = 1.0f - m1boost;
				}
				for (int s_next = 0; s_next < numStates; s_next++) {
					//System.out.println("    s_next="+s_next + " b(j+1,s_next)="+ betas.get(j+1, s_next) + " * " +
					//		hmm.getTransitionProb(s, s_next) + " * " + hmm.getEmissionProb(s_next, j));
					float trans = getTransitionProb(s, s_next);
					if (use_m1) {
						if (s <= l && s > 0 && m1a.aligned(j-1, s-1))
							trans = m1boost;
						else
							trans *= m1penalty;
					}
					beta += betas.get(j+1, s_next) *
					  trans *
					  getEmissionProb(j+1, s_next);
				}
				
				beta /= anorms[j];
				//System.out.println("  s="+s+ "  b:"+beta);
				betas.set(j, s, beta);
			}
		}
		
		// PARTIAL COUNTS FOR EMMISSIONS (WORD TRANSLATION)
		float totalProb[] = new float[J];
		for (int j=1; j<J; j++) {
			float tp = 0.0f;
			for (int s = 0; s < numStates; s++) {
				tp += betas.get(j, s) * alphas.get(j, s);
			}
			// System.out.println("total prob(" + j + ")=" + tp);
			totalProb[j] = tp;
			for (int s = 0; s < numStates; s++) {
				// j=1 s=14
				int iplus1 = e_coords.get(j, s);
				if (iplus1 == -1) continue;
				float pc = betas.get(j, s) * alphas.get(j, s) / tp;
				if (pg != null) {
					int e = 0;
					if (s <= l)
						e = s;
					if (s != 0) {
						float p = pg.getAlignmentPointPosterior(j-1, e) + pc;
						pg.setAlignmentPointPosterior(j-1, e, p);
					}
				} else {
					try {
						addTranslationCount(iplus1, j-1, pc);
					} catch (Exception e) {
						throw new RuntimeException("J=" + J + ", numStates=" + numStates +": Failed to add (" +iplus1+","+(j-1)+") += " + pc + " s=" + s + " pp=" + pp + "\n E:\n"+ e_coords);
					}
				}
				//System.out.println("ec="+ec+" pc="+pc);
			}
		}
		
		// PARTIAL COUNTS FOR TRANSITIONS
		if (pg == null) {
			for (int j=1; j<J-1; j++) {
				for (int s_prev=0; s_prev < numStates; s_prev++) {
					for (int s=0; s < numStates; s++) {
						int tc = transition_coords.get(s_prev, s);
						if (tc == -1) continue;
						float m1boost = 1.0f;
						float m1penalty = 0.0f;
						boolean use_m1 = false;
						if (m1a != null && m1a.isFAligned(j-1)) {
							float m1post = 0.0f;
							use_m1 = true;
							for (int i=0; i<l; i++)
								if (m1a.aligned(j-1, i))
									m1post = m1_post.getAlignmentPointPosterior(j-1, i+1);
							m1boost = (float)(Math.sqrt(m1post));
							m1penalty = 1.0f - m1boost;
						}
						float trans = getTransitionProb(s_prev, s);
						if (use_m1) {
							if (s <= l && s > 0 && m1a.aligned(j-1, s-1))
								trans = m1boost;
							else
								trans *= m1penalty;
						}
						// SKIPPING: REMOVE!!!
						if (use_m1) continue;
						float pc = alphas.get(j, s_prev)
							* trans
							* emission.get(j+1, s)
							/ anorms[j+1]
							* betas.get(j+1, s)
							/ totalProb[j+1];
						acounts.add(tc, (char)l, pc);
						//System.out.println("tc="+tc+"  pc="+pc);
					}
				}
			}
		}
		
		float tlp = 0.0f;
		for (float n : anorms)
			tlp += Math.log(n);
		return tlp;
		//System.out.println(acounts);
		
//		System.out.println(alphas + "\n" + betas);
	}

	@Override
	public AlignmentPosteriorGrid computeAlignmentPosteriors(PhrasePair pp) {
		AlignmentPosteriorGrid res = new AlignmentPosteriorGrid(pp);
		buildHMMTables(pp);
		baumWelch(pp, res);
		return res;
	}

	@Override
	public Alignment viterbiAlign(PhrasePair sentence,
			PerplexityReporter reporter) {
		this.buildHMMTables(sentence);
		Alignment res = new Alignment(sentence.getF().size(), sentence.getE().size());
		int J = sentence.getF().size() + 1;
		int numStates = getNumStates();
		viterbi.resize(J, getNumStates());
		backtrace.resize(J, getNumStates());
		viterbi.fill(Float.NEGATIVE_INFINITY);
		viterbi.set(0, 0, 0.0f);
		int lene = sentence.getE().getWords().length;
		Alignment m1a = null;
		if (m1_post != null)
			m1a = m1_post.alignPosteriorThreshold(THRESH);

		//System.out.println(emission);
		for (int j = 1; j < J; j++) {
			//System.out.println("J="+j);
			boolean valid = false;
			for (int s = 1; s < numStates; s++) {
				float best = Float.NEGATIVE_INFINITY;
				int best_s = -1;
				double emitLogProb = Math.log(emission.get(j, s));
				if (emitLogProb == Float.NEGATIVE_INFINITY) {
					//System.out.println("BAD STATE: " + j + " " + s);
					continue;
				}
				//System.out.println("j="+j + " s="+s+ "  ep"+emitLogProb);
				for (int s_prev = 0; s_prev < numStates; s_prev++) {
					float m1boost = 1.0f;
					float m1penalty = 0.0f;
					boolean use_m1 = false;
					if (m1a != null && m1a.isFAligned(j-1)) {
						float m1post = 0.0f;
						use_m1 = true;
						for (int i=0; i<lene; i++) {
							if (m1a.aligned(j-1, i))
								m1post = m1_post.getAlignmentPointPosterior(j-1, i+1);
						}
						m1boost = (float)Math.sqrt(m1post);
						m1penalty = 1.0f - m1boost;
					}
					float trans = getTransitionProb(s_prev, s);
					if (use_m1) {
						if (s <= l && s > 0 && m1a.aligned(j-1, s-1))
							trans = m1boost;
						else
							trans *= m1penalty;
					}
					float cur = (float)(viterbi.get(j - 1, s_prev) +
					    Math.log(trans) +
					    emitLogProb);
					//System.out.println(" s'="+s_prev + "  cur="+cur);
					if (cur > best) {
						best = cur;
						best_s = s_prev;
						//System.out.println("new best: " + s + " " + best_s);
					}
				}
				//System.out.println(" s_best="+best_s + "  cur="+best);
				viterbi.set(j, s, best);
				if (best != Float.NEGATIVE_INFINITY)
					valid = true;
				backtrace.set(j, s, best_s);
			}
			// if we don't know how to generate some column
			// create a uniform distribution over the states
			// and assume the previous state was the best
			if (!valid) {
				float best = Float.NEGATIVE_INFINITY;
				int bests = -1;
				for (int s = 1; s < numStates; s++) {
					if (viterbi.get(j-1, s) > best) {
						best = viterbi.get(j-1, s);
						bests = s;
					}
				}
				for (int s = 1; s < numStates; s++) {
					viterbi.set(j, s, 0.0f);
					backtrace.set(j, s, bests);
				}
			}
		}
		//System.out.println(viterbi);
		float best = Float.NEGATIVE_INFINITY;
		int best_s = -1;
		for (int s = 1; s < numStates; s++) {
			if (viterbi.get(J-1, s) > best) {
				best = viterbi.get(J-1,s);
				best_s = s;
			}
		}
		//System.out.println("vit: " + best + "j-1="+(J-1));
		reporter.addFactor(best, J - 1);
		//System.out.println(viterbi);
		int e = best_s;
		for (int f=J-1; f>0; f--) {
			if (e <= 0) {
				throw new ZeroProbabilityException("  Error f=" +f+" e="+e+
						"  sentence + \n" + viterbi + "\n" + emission + "\n" + transition + "\n" + backtrace);
			} else {
				if (viterbi.get(f, e) < 0.0) {
					// hack to avoid errors
					try {
						int af = f-1;
						int ae = e_words.get(f, e);
						if (ae >= 0)
							res.align(af, ae);
						//else
						//	System.err.println("ALIGN NULL TO " + af);
					} catch (RuntimeException ex) {
						throw new RuntimeException("Caught " + ex + "\nvit(f,e)="+viterbi.get(f,e)+"  size(f,e)=" + sentence.getF().size() +","+ sentence.getE().size() + " Error f=" +f+" e="+e+
								"  sentence + \n" + viterbi + "\n" + emission + "\n" + transition + "\n" + backtrace + "\n" + e_words);
					}
				}
				e = backtrace.get(f, e);
			}
		}
		return res;
	}
}
