package edu.umd.hooka.alignment;

import edu.umd.hooka.Alignment;

public class RefinerFactory {
	static final String GROW_DIAG_FINAL_AND = "grow-diag-final-and";
	static final String INTERSECTION = "intersection";
	static final String UNION = "union";
	static final String GROW_DIAG_FINAL = "grow-diag-final";
	static final String OCH = "och";
	
	static abstract class Pred {
		public abstract boolean eval(Alignment a, int i, int j);
	}
	
	static class Och extends Pred {
		public boolean eval(Alignment a, int i, int j) {
			return (!a.rookAligned(i, j) ||
					a.neighborAligned(i, j) && !a.lneighborAligned(i, j));		  
		}
	}

	static class Koehn extends Pred {
		public boolean eval(Alignment a, int i, int j) {
			return !a.doubleRookAligned(i, j) && a.neighborAligned(i, j);		  
		}
	}

	static class KoehnFinal extends Pred {
		public boolean eval(Alignment a, int i, int j) {
			return !a.rookAligned(i, j);		  
		}
	}

	static class IntersectionRefiner extends Refiner {
		public Alignment refine(Alignment a1, Alignment a2) {
			return Alignment.intersect(a1, a2);
		}
	}

	static class UnionRefiner extends Refiner {
		public Alignment refine(Alignment a1, Alignment a2) {
			return Alignment.union(a1, a2);
		}
	}

	static class GrowDiagFinalAndRefiner extends Refiner {
		static Koehn koehn = new Koehn();
		static KoehnFinal koehnFinal = new KoehnFinal();
		public Alignment refine(Alignment a2, Alignment a1) {
			Alignment au = Alignment.union(a1, a2);
			Alignment a = Alignment.intersect(a1, a2);
			
			grow(a, koehn, false, au);
			//System.out.println(a1.toString());
			grow(a, koehnFinal, true, a1);
			grow(a, koehnFinal, true, a2);
			
			return a;
		}
	}
	
	static class Pair {
		public Pair(int i, int j) {this.i=i; this.j=j;}
		public int i;
		public int j;
	}
	
	static void grow(Alignment a, Pred pred, boolean idem, Alignment pot)
	{
		int flen = a.getFLength();
		int elen = a.getELength();
		if (idem) {
			for (int i=0;i<flen;i++)
				for (int j=0;j<elen;j++)
					if (pot.aligned(i,j) &&
							!a.aligned(i, j) &&
							pred.eval(a, i, j))
						a.align(i, j);
		} else {
			java.util.ArrayList<Pair> p = new java.util.ArrayList<Pair>();
			for (int i=0;i<flen;i++)
				for (int j=0;j<elen;j++)
					if (pot.aligned(i, j) &&
							!a.aligned(i, j))
						p.add(new Pair(i,j));
			int plen = p.size();
			Pair[] pairs = new Pair[plen]; p.toArray(pairs); p = null;
			while (true) {
				int cur = 0;
				boolean flag = false;
				for (int pi=0; pi<plen; pi++)
				{
					Pair pp = pairs[pi];
					if (pred.eval(a, pp.i, pp.j)) {
						a.align(pp.i, pp.j);
						flag = true;
					} else {
						pairs[cur] = pp;
						cur++;
					}
				}
				plen = cur;
				if (!flag)
					break;
			}
		}
	}

	public static Refiner getForName(String name) throws Exception
	{
		if (name.equals(INTERSECTION))
			return new IntersectionRefiner();
		else if (name.equals(UNION))
			return new UnionRefiner();
		else if (name.equals(GROW_DIAG_FINAL_AND))
			return new GrowDiagFinalAndRefiner();
		else
			throw new Exception("Unknown refinement algorithm: " + name);
	}
}
