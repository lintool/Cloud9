package edu.umd.cloud9.collection.gov2;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import edu.umd.cloud9.collection.Indexable;

public class Gov2Document implements Writable, Indexable {

	/**
	 * Start delimiter of the document, which is &lt;<code>DOC</code>&gt;.
	 */
	public static final String XML_START_TAG = "<DOC>";

	/**
	 * End delimiter of the document, which is &lt;<code>/DOC</code>&gt;.
	 */
	public static final String XML_END_TAG = "</DOC>";

	private String mDocid;
	private String mContent;

	/**
	 * Creates an empty <code>Doc2Document</code> object.
	 */
	public Gov2Document() {
		try {
			startTag = XML_START_TAG.getBytes("utf-8");
			endTag = XML_END_TAG.getBytes("utf-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Deserializes this object.
	 */
	public void write(DataOutput out) throws IOException {
		out.writeUTF(mDocid);
		byte[] bytes = mContent.getBytes();
		WritableUtils.writeVInt(out, bytes.length);
		out.write(bytes, 0, bytes.length);
	}

	/**
	 * Serializes this object.
	 */
	public void readFields(DataInput in) throws IOException {
		mDocid = in.readUTF();
		int length = WritableUtils.readVInt(in);
		byte[] bytes = new byte[length];
		in.readFully(bytes, 0, length);
		mContent = new String(bytes);
		// Gov2Document.readDocument(this, new String(bytes));
	}

	/**
	 * Returns the docid of this Gov2 document.
	 */
	public String getDocid() {
		return mDocid;
	}

	/**
	 * Returns the content of this Gov2 document.
	 */
	public String getContent() {
		return mContent;
	}

	/**
	 * Reads a raw XML string into a <code>Gov2Document</code> object.
	 * 
	 * @param doc
	 *            the <code>Gov2Document</code> object
	 * @param s
	 *            raw XML string
	 */
	public static void readDocument(Gov2Document doc, String s) {
		if (s == null) {
			throw new RuntimeException("Error, can't read null string!");
		}

		int start = s.indexOf("<DOCNO>");

		if (start == -1) {
			throw new RuntimeException("Unable to find DOCNO tag!");
		} else {
			int end = s.indexOf("</DOCNO>", start);

			doc.mDocid = s.substring(start + 7, end);
			// System.out.println("docid: " + doc.mDocid);
		}

		start = s.indexOf("</DOCHDR>");

		if (start == -1) {
			throw new RuntimeException("Unable to find DOCHDR tag!");
		} else {
			int end = s.indexOf("</DOC>", start);

			doc.mContent = scrubHTML(s.substring(start + 9, end));
			// System.out.println("content: " + doc.mContent);
		}

	}

	private static DataInputStream fsin;
	private static byte[] startTag;
	private static byte[] endTag;
	private static DataOutputBuffer buffer = new DataOutputBuffer();

	public static boolean readNextGov2Document(Gov2Document doc, DataInputStream stream)
			throws IOException {
		fsin = stream;

		if (readUntilMatch(startTag, false)) {
			try {
				buffer.write(startTag);
				if (readUntilMatch(endTag, true)) {
					String s = new String(buffer.getData());

					// System.out.println("DOC: " + contents);
					readDocument(doc, s);

					// key.set(fsin.getPos());
					// value.set(buffer.getData(), 0, buffer.getLength());

					return true;
				}
			} finally {
				buffer.reset();
			}
		}

		return false;
	}

	private static boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
		int i = 0;
		while (true) {
			int b = fsin.read();
			// end of file:
			if (b == -1)
				return false;
			// save to buffer:
			if (withinBlock)
				buffer.write(b);

			// check if we're matching:
			if (b == match[i]) {
				i++;
				if (i >= match.length)
					return true;
			} else
				i = 0;
			// see if we've passed the stop point:
			// if (!withinBlock && i == 0 && fsin.getPos() >= end)
			// return false;
		}
	}

	private static Pattern PATTERN_BODY = Pattern.compile("<[Bb][Oo][Dd][Yy](.*)", Pattern.DOTALL);
	private static Pattern PATTERN_JAVASCRIPT = Pattern.compile("<script(.*?)</script>",
			Pattern.DOTALL | Pattern.CASE_INSENSITIVE);
	private static Pattern PATTERN_STYLE = Pattern.compile("<style(.*?)</style>", Pattern.DOTALL
			| Pattern.CASE_INSENSITIVE);
	private static Pattern PATTERN_COMMENTS = Pattern.compile("<!--(.*?)-->", Pattern.DOTALL);
	private static Pattern PATTERN_ALL_HTML_TAGS = Pattern.compile("<(.*?)>", Pattern.DOTALL);
	private static Pattern PATTERN_NBSP = Pattern
			.compile("(&nbsp;|&gt;|&lt;|&quot;|&raquo;|&laquo;|&lsaquo;|&rsaquo;|&mdidot;|&mdash;|&amp;)");

	private static Pattern PATTERN_APOSTROPHE = Pattern.compile("&#0?39;");

	private static Pattern PATTERN_CRLF = Pattern.compile("\\n\\r");
	private static Pattern PATTERN_MULTIPLE_BLANK_LINES = Pattern.compile("\\n{3,}");
	private static Pattern PATTERN_SPACES = Pattern.compile("[\\t ]+");
	private static Pattern PATTERN_LEADING_SPACES = Pattern.compile("^[ \\t]+", Pattern.MULTILINE);

	static private String scrubHTML(String raw) {
		Matcher m = PATTERN_BODY.matcher(raw);

		if (!m.find())
			return "";

		String s = null;
		s = m.group();

		s = PATTERN_JAVASCRIPT.matcher(s).replaceAll(" ");
		s = PATTERN_STYLE.matcher(s).replaceAll(" ");
		// strip comments before all HTML tags because you can comment out
		// multiple HTML tags
		s = PATTERN_COMMENTS.matcher(s).replaceAll(" ");
		s = PATTERN_ALL_HTML_TAGS.matcher(s).replaceAll(" ");
		s = PATTERN_NBSP.matcher(s).replaceAll(" ");
		s = PATTERN_APOSTROPHE.matcher(s).replaceAll("'");
		s = PATTERN_SPACES.matcher(s).replaceAll(" ");
		s = PATTERN_LEADING_SPACES.matcher(s).replaceAll("");
		s = PATTERN_CRLF.matcher(s).replaceAll("\n");
		s = PATTERN_MULTIPLE_BLANK_LINES.matcher(s).replaceAll("\n\n");

		return s;

	}
}
