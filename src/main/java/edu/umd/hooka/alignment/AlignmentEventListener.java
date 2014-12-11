package edu.umd.hooka.alignment;

import edu.umd.hooka.PhrasePair;

public interface AlignmentEventListener {

	public void notifyUnalignablePair(PhrasePair pp, String reason);

}
