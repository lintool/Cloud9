package edu.umd.hooka.alignment;

import edu.umd.hooka.Alignment;

public abstract class Refiner {
	public abstract Alignment refine(Alignment a1, Alignment a2);
}
