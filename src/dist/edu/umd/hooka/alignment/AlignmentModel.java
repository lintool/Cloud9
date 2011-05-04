package edu.umd.hooka.alignment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.umd.hooka.Alignment;
import edu.umd.hooka.AlignmentPosteriorGrid;
import edu.umd.hooka.PhrasePair;

public abstract class AlignmentModel {

	private List<AlignmentEventListener> _listeners =
		new ArrayList<AlignmentEventListener>();

	public void addAlignmentListener(AlignmentEventListener ael) {
		_listeners.add(ael);
	}
	
	protected void notifyUnalignablePair(PhrasePair pp, String reason) {
		for (AlignmentEventListener l : _listeners) {
			l.notifyUnalignablePair(pp, reason);
		}
	}
	
	public abstract void clearModel();
	public abstract void processTrainingInstance(PhrasePair pp, Reporter r);
	public abstract void writePartialCounts(OutputCollector<IntWritable,PartialCountContainer> output) throws IOException;
	
	public abstract Alignment viterbiAlign(PhrasePair pp, PerplexityReporter reporter);
	
	public abstract AlignmentPosteriorGrid computeAlignmentPosteriors(PhrasePair pp);
}
