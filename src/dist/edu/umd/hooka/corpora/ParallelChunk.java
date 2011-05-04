package edu.umd.hooka.corpora;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.umd.hooka.alignment.aer.ReferenceAlignment;

//this should be storing two chunks, 1 from src and 1 from target.

//This is basically a data structure for the xml-type format of the input bitext 
//
//e.g.
//<pchunk name="eu+nc_9">
//<s lang="de"> Wie Sie sicher aus der Presse und dem Fernsehen wissen , gab es in Sri Lanka mehrere Bombenexplosionen mit zahlreichen Toten . </s>
//<s lang="en"> You will be aware from the press and television that there have been a number of bomb explosions and killings in Sri Lanka . </s>
//</pchunk>

public class ParallelChunk {
	public final static String escape(String s) {
		return s.replaceAll("\\&", "&amp;").replaceAll("<", "&lt;")
		 .replaceAll(">", "&gt;");
	}

	public HashMap<Language, Chunk> l2c = new HashMap<Language, Chunk>();
	public HashMap<LanguagePair, ReferenceAlignment> lp2ra = null;
	public String name;
	public void addChunk(Language l, Chunk c) {
		if (l2c.containsKey(l)) {
			throw new RuntimeException("PChunk " + this + " already contains language " + l);
		}
		l2c.put(l, c);
	}
	public void addReferenceAlignment(LanguagePair lp, ReferenceAlignment r) {
		if (lp2ra == null)
			lp2ra = new HashMap<LanguagePair, ReferenceAlignment>(1);
		lp2ra.put(lp, r);
	}
	public ReferenceAlignment getReferenceAlignment(LanguagePair lp) {
		if (lp2ra == null) return null;

		ReferenceAlignment r = lp2ra.get(lp);
		if (r == null) {
			//if can't get alignment (for en-fr), then try for other translation direction (fr-en), and transpose the alignment for those.
			r = lp2ra.get(lp.inverted());
			if (r != null) {
				r = (ReferenceAlignment)r.getTranspose();
				lp2ra.put(lp, r);
			}
		}
		return r;
	}
	public final Chunk getChunk(Language l) {
		return l2c.get(l);
	}
	public String toString() {
		ArrayList<Integer> lens = new ArrayList<Integer>();
		for (Chunk c : l2c.values())
			lens.add(c.getLength());
		return "PChunk<langs=" + l2c.keySet() +
		" chunk lengths (words)=" + lens +
		(lp2ra == null ? "" : " refaligns="+lp2ra.keySet()) + ">";
	}
	public void setName(String name) {this.name = name; }
	public String getName() { return name; }
	
	@Override
	public int hashCode() {
		return (lp2ra == null ? 0 : lp2ra.hashCode() * 31) + l2c.hashCode();
	}
	
	final public String idString() {
		long l = 1;
		for (Map.Entry<Language,Chunk> p : l2c.entrySet()) {
			l *= 31;
			l += p.getKey().hashCode() * 17;
			l += p.getValue().hashCode();
		}
		if (lp2ra != null) {
			for (Map.Entry<LanguagePair,ReferenceAlignment> p : lp2ra.entrySet()) {
				l *= 29;
				l += p.getKey().hashCode();
			}
		}
		return Long.toString(l, Character.MAX_RADIX) + "_" + name;
	}
			
	public String toXML() {
		StringBuffer sb = new StringBuffer();
		sb.append("<pchunk");
		if (name != null) sb.append(" name=\"").append(name).append('"');
		sb.append(">\n");
		for (Map.Entry<Language,Chunk> p : l2c.entrySet()) {
			sb.append("  <s lang=\"").append(p.getKey().code());
			sb.append("\"> ").append(escape(p.getValue().toString())).append(" </s>\n");
		}
		if (lp2ra != null) {
			for (Map.Entry<LanguagePair,ReferenceAlignment> p : lp2ra.entrySet()) {
				sb.append("  <wordalignment langpair=\"").append(p.getKey());
				sb.append("\"> ").append(p.getValue()).append(" </wordalignment>\n");
			}
		}
		sb.append("</pchunk>\n");
		return sb.toString();
	}

}
