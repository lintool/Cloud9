package edu.umd.hooka.alignment.model1;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;

import edu.umd.hooka.Int2FloatMap;
import edu.umd.hooka.PhrasePair;
import edu.umd.hooka.alignment.AlignmentModel;
import edu.umd.hooka.alignment.PartialCountContainer;
import edu.umd.hooka.ttables.TTable;

public abstract class Model1Base extends AlignmentModel {

	protected final int maxF = 214350; // TODO: fix
	protected boolean _includeEnglishNullWord = true;

	public Model1Base(boolean incNullWord) {
		_includeEnglishNullWord = incNullWord;
	}

	TreeMap<IntWritable, Int2FloatMap> counts =
		new TreeMap<IntWritable, Int2FloatMap>();
	IntWritable nullWord = new IntWritable(0);

	public void writePartialCounts(OutputCollector<IntWritable,PartialCountContainer> output) throws IOException
	{
		PartialCountContainer pcc = new PartialCountContainer();
		Iterator<Map.Entry<IntWritable, Int2FloatMap>> i = counts.entrySet().iterator();
		while (i.hasNext()) {
			Map.Entry<IntWritable, Int2FloatMap> p = i.next();
			pcc.setContent(p.getValue().getAsIndexedFloatArray());
			output.collect(p.getKey(), pcc);
			i.remove();
		}
	}
	
	public void addPartialTranslationCountsToTTable(TTable tcounts) {
		Iterator<Map.Entry<IntWritable, Int2FloatMap>> i = counts.entrySet().iterator();
		while (i.hasNext()) {
			Map.Entry<IntWritable, Int2FloatMap> p = i.next();
			int ei = p.getKey().get();
			for (Map.Entry<Integer, FloatWritable> f:p.getValue().entrySet()) {
				tcounts.add(ei, f.getKey(), f.getValue().get());
			}
			i.remove();
		}
	}
	
	/*tcmap is actually a 2D array projected onto a linear space*/	
	FloatWritable[] tcmap = null;
	int width = 0;
	protected void initializeCountTableForSentencePair(PhrasePair pp) {
		int ew[] = pp.getE().getWords();
		int fw[] = pp.getF().getWords();
		width = fw.length;
		// add null word to the beginning of e sentence:
		tcmap = new FloatWritable[(ew.length+1) * fw.length];
		int c = 0;
		Int2FloatMap ecm = null;
		if (_includeEnglishNullWord) {
			ecm = counts.get(nullWord);
			if (ecm == null) {
				ecm = new Int2FloatMap();
				counts.put(nullWord, ecm);
			}
			for (int fi:fw) {
				ecm.createIfMissing(fi);
				tcmap[c] = ecm.getFloatWritable(fi);
				c++;
			}
		} else { c += fw.length; }
		for (int ei:ew) {
			IntWritable cew = new IntWritable(ei);
			ecm = counts.get(cew);
			if (ecm == null) {
				ecm = new Int2FloatMap();
				counts.put(cew, ecm);
			}
			for (int fi:fw) {
				ecm.createIfMissing(fi);
				tcmap[c] = ecm.getFloatWritable(fi);
				c++;
			}
		}
	}
	
	/**
	 * Normally, i is a zero based array, but since E
	 * may have a null word, in this case, i=0 refers to the
	 * null word, i=1 refers to the first word.  For j,
	 * j=0 refers to the first word.
	 */
	protected final int getTranslationCoord(int i_plus1, int j) {
		return i_plus1 * width + j;
	}
	
	protected final void addTranslationCount(int i_plus1, int j, float v) {
		if (v == 0.0f) return;
		int coord = getTranslationCoord(i_plus1, j);
		if (tcmap[coord] == null) {
			throw new RuntimeException("isNull(" + i_plus1 + "," + j +")");
		}
		//add v to existing count
		tcmap[coord].set(tcmap[coord].get() + v);
	}

	protected final void addTranslationCount(int coord, float v) {
		if (v == 0.0f) return;
		tcmap[coord].set(tcmap[coord].get() + v);
	}
}
