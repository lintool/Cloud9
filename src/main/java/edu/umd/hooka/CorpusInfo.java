package edu.umd.hooka;

import org.apache.hadoop.fs.Path;

public abstract class CorpusInfo {
	public enum Corpus { HANSARDS,
		ARABIC_SMALL,
		ARABIC_10k,
		ARABIC_50k,
		ARABIC_150k,
		ARABIC_500k,
		ARABIC_1000k,
		ARABIC_1500k,
		ARABIC_5000k,
		ARABIC_LARGE,
		CZECH_WMT08,
		GERMAN_TINY
	};
	public static CorpusInfo getCorpus(Corpus corpus) {
		CorpusInfo res = null;
		switch (corpus) {
			case HANSARDS:
				res = new Hansards();
				break;
			case CZECH_WMT08:
				res = new CzechWMT08();
				break;
			case ARABIC_SMALL:
				res = new ArabicSmall();
				break;
			case ARABIC_10k:
				res = new Arabic10k();
				break;
			case ARABIC_50k:
				res = new Arabic50k();
				break;
			case ARABIC_150k:
				res = new Arabic150k();
				break;
			case ARABIC_500k:
				res = new Arabic500k();
				break;
			case ARABIC_1000k:
				res = new Arabic1000k();
				break;
			case ARABIC_1500k:
				res = new Arabic1500k();
				break;
			case ARABIC_LARGE:
			case ARABIC_5000k:
				res = new ArabicLarge();
				break;
			case GERMAN_TINY:
				res = new GermanTiny();
				break;
		}
		return res;
	}
	
	protected abstract String getBasePath();
	protected abstract String getBaseName();
	public Path getBitext()  {
		return new Path(getBasePath() + Path.SEPARATOR + getBaseName() + ".bitext");
	}
	public Path getAlignedBitext() {
		return new Path(getBasePath() + Path.SEPARATOR + getBaseName() + ".bitext-aligned");	
	}
	public Path getCanonicalTTable() {
		return new Path(getBasePath() + Path.SEPARATOR + getLocalTTable());
	}
	public Path getCanonicalTTable(String type) {
		return new Path(getBasePath() + Path.SEPARATOR + getLocalTTable(type));
	}
	public Path getLocalTTable() {
		return new Path(getBaseName() + ".ttable");
	}
	public Path getLocalTTable(String type) {
		return new Path(getBaseName() + ".ttable-" + type);
	}
	public Path getLocalATable() {
		return new Path(getBaseName() + ".atable");
	}
	public Path getLocalPhraseTable() {
		return new Path(getBaseName() + ".ptable");
	}
	public Path getTestSubset() {
		return new Path(getBasePath() + Path.SEPARATOR + getBaseName() + ".test");
	}

	static class Hansards extends CorpusInfo {
		public String getBasePath() {
			return "/shared/bitexts/hansards.fr-en";
		}
		public String getBaseName() {
			return "hansards.aachen";
		}
	}

	static class CzechWMT08 extends CorpusInfo {
		public String getBasePath() {
			return "/shared/bitexts/cs-en.wmt08";
		}
		public String getBaseName() {
			return "cs-en";
		}
	}

	static class ArabicSmall extends CorpusInfo {
		public String getBasePath() {
			return "/shared/bitexts/small.ar-en.ldc";
		}
		public String getBaseName() {
			return "small.ar-en";
		}
	}

	static class ArabicLarge extends CorpusInfo {
		public String getBasePath() {
			return "/shared/bitexts/large.ar-en.ldc";
		}
		public String getBaseName() {
			return "large.ar-en";
		}
	}

	static class Arabic10k extends CorpusInfo {
		public String getBasePath() {
			return "/shared/bitexts/ar-en.ldc.10k2";
		}
		public String getBaseName() {
			return "ar-en.10k";
		}
	}

	static class Arabic50k extends CorpusInfo {
		public String getBasePath() {
			return "/shared/bitexts/ar-en.ldc.50k";
		}
		public String getBaseName() {
			return "ar-en.50k";
		}
	}
	static class Arabic150k extends CorpusInfo {
		public String getBasePath() {
			return "/shared/bitexts/ar-en.ldc.150k";
		}
		public String getBaseName() {
			return "ar-en.150k";
		}
	}

	static class Arabic500k extends CorpusInfo {
		public String getBasePath() {
			return "/shared/bitexts/ar-en.ldc.500k";
		}
		public String getBaseName() {
			return "ar-en.500k";
		}
	}

	static class Arabic1500k extends CorpusInfo {
		public String getBasePath() {
			return "/shared/bitexts/ar-en.ldc.1500k";
		}
		public String getBaseName() {
			return "ar-en.1500k";
		}
	}

	static class Arabic1000k extends CorpusInfo {
		public String getBasePath() {
			return "/shared/bitexts/ar-en.ldc.1000k";
		}
		public String getBaseName() {
			return "ar-en.1000k";
		}
	}

	static class GermanTiny extends CorpusInfo {
		public String getBasePath() {
			return "/shared/bitexts/tiny.de-en";
		}
		public String getBaseName() {
			return "tiny-deen";
		}
	}
}
