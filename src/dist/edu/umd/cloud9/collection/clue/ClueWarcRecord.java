/**
 * Container for a generic Warc Record 
 * 
 * (C) 2009 - Carnegie Mellon University
 * 
 * 1. Redistributions of this source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer. 
 * 2. The names "Lemur", "Indri", "University of Massachusetts",  
 *    "Carnegie Mellon", and "lemurproject" must not be used to 
 *    endorse or promote products derived from this software without
 *    prior written permission. To obtain permission, contact 
 *    license@lemurproject.org.
 *
 * 4. Products derived from this software may not be called "Lemur" or "Indri"
 *    nor may "Lemur" or "Indri" appear in their names without prior written
 *    permission of The Lemur Project. To obtain permission,
 *    contact license@lemurproject.org.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE LEMUR PROJECT AS PART OF THE CLUEWEB09
 * PROJECT AND OTHER CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED 
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF 
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN 
 * NO EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY 
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS 
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) 
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING 
 * IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE 
 * POSSIBILITY OF SUCH DAMAGE. 
 * 
 * @author mhoy@cs.cmu.edu (Mark J. Hoy)
 */

package edu.umd.cloud9.collection.clue;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import edu.umd.cloud9.collection.Indexable;

public class ClueWarcRecord implements Writable, Indexable {
	private static final Logger sLogger = Logger.getLogger(ClueWarcRecord.class);

	public static String WARC_VERSION = "WARC/0.18";
	public static String WARC_VERSION_LINE = "WARC/0.18\n";
	private static String NEWLINE = "\n";

	private static byte MASK_THREE_BYTE_CHAR = (byte) (0xE0);
	private static byte MASK_TWO_BYTE_CHAR = (byte) (0xC0);
	private static byte MASK_TOPMOST_BIT = (byte) (0x80);
	private static byte MASK_BOTTOM_SIX_BITS = (byte) (0x1F);
	private static byte MASK_BOTTOM_FIVE_BITS = (byte) (0x3F);
	private static byte MASK_BOTTOM_FOUR_BITS = (byte) (0x0F);

	/**
	 * Our read line implementation. We cannot allow buffering here (for gzip
	 * streams) so, we need to use DataInputStream. Also - we need to account
	 * for java's UTF8 implementation
	 * 
	 * @param in
	 *            the input data stream
	 * @return the read line (or null if eof)
	 * @throws java.io.IOException
	 */
	private static String readLineFromInputStream(DataInputStream in) throws IOException {
		StringBuilder retString = new StringBuilder();

		boolean keepReading = true;
		try {
			do {
				char thisChar = 0;
				byte readByte = in.readByte();

				// check to see if it's a multibyte character
				if ((readByte & MASK_THREE_BYTE_CHAR) == MASK_THREE_BYTE_CHAR) {
					// need to read the next 2 bytes
					if (in.available() < 2) {
						// treat these all as individual characters
						retString.append((char) readByte);
						int numAvailable = in.available();
						for (int i = 0; i < numAvailable; i++) {
							retString.append((char) (in.readByte()));
						}
						continue;
					}
					byte secondByte = in.readByte();
					byte thirdByte = in.readByte();
					// ensure the topmost bit is set
					if (((secondByte & MASK_TOPMOST_BIT) != MASK_TOPMOST_BIT)
							|| ((thirdByte & MASK_TOPMOST_BIT) != MASK_TOPMOST_BIT)) {
						// treat these as individual characters
						retString.append((char) readByte);
						retString.append((char) secondByte);
						retString.append((char) thirdByte);
						continue;
					}
					int finalVal = (thirdByte & MASK_BOTTOM_FIVE_BITS) + 64
							* (secondByte & MASK_BOTTOM_FIVE_BITS) + 4096
							* (readByte & MASK_BOTTOM_FOUR_BITS);
					thisChar = (char) finalVal;
				} else if ((readByte & MASK_TWO_BYTE_CHAR) == MASK_TWO_BYTE_CHAR) {
					// need to read next byte
					if (in.available() < 1) {
						// treat this as individual characters
						retString.append((char) readByte);
						continue;
					}
					byte secondByte = in.readByte();
					if ((secondByte & MASK_TOPMOST_BIT) != MASK_TOPMOST_BIT) {
						retString.append((char) readByte);
						retString.append((char) secondByte);
						continue;
					}
					int finalVal = (secondByte & MASK_BOTTOM_FIVE_BITS) + 64
							* (readByte & MASK_BOTTOM_SIX_BITS);
					thisChar = (char) finalVal;
				} else {
					// interpret it as a single byte
					thisChar = (char) readByte;
				}

				if (thisChar == '\n') {
					keepReading = false;
				} else {
					retString.append(thisChar);
				}
			} while (keepReading);
		} catch (EOFException eofEx) {
			return null;
		}

		if (retString.length() == 0) {
			return "";
		}

		return retString.toString();
	}

	static Set<String> errors = new HashSet<String>();
	static {
		errors.add("clueweb09-en0001-41-14941");
		errors.add("clueweb09-en0003-88-28589");
		errors.add("clueweb09-en0005-46-08669");
		errors.add("clueweb09-en0007-31-02866");
		errors.add("clueweb09-en0007-91-00093");
		errors.add("clueweb09-en0007-93-23823");
		errors.add("clueweb09-en0009-60-41300");
		errors.add("clueweb09-en0009-60-41302");
	}

	/**
	 * The actual heavy lifting of reading in the next WARC record
	 * 
	 * @param in
	 *            the data input stream
	 * @param headerBuffer
	 *            a blank string buffer to contain the WARC header
	 * @return the content byts (w/ the headerBuffer populated)
	 * @throws java.io.IOException
	 */
	private static byte[] readNextRecord(DataInputStream in, StringBuffer headerBuffer)
			throws IOException {
		if (in == null) {
			return null;
		}
		if (headerBuffer == null) {
			return null;
		}

		String line = null;
		boolean foundMark = false;
		boolean inHeader = true;
		byte[] retContent = null;

		// cannot be using a buffered reader here!!!!
		// just read the header
		// first - find our WARC header
		while ((!foundMark) && ((line = readLineFromInputStream(in)) != null)) {
			if (line.startsWith(WARC_VERSION)) {
				foundMark = true;
			}
		}

		// no WARC mark?
		if (!foundMark) {
			return null;
		}

		boolean ignoreFirstEmptyLine = false;
		if (previousTrecid != null && errors.contains(previousTrecid)) {
			sLogger.info("Special handling of errors following record " + previousTrecid);
			ignoreFirstEmptyLine = true;
		}

		// then read to the first newline
		// get the content length and set our retContent
		while (inHeader && ((line = readLineFromInputStream(in)) != null)) {
			if (line.trim().length() == 0 && ignoreFirstEmptyLine) {
				ignoreFirstEmptyLine = false;
				continue;
			}

			if (line.trim().length() == 0) {
				inHeader = false;
			} else {
				// System.out.println("appending");
				headerBuffer.append(line);
				headerBuffer.append(NEWLINE);
			}
		}

		// ok - we've got our header - find the content length
		// designated by Content-Length: <length>
		String[] headerPieces = headerBuffer.toString().split(NEWLINE);
		int contentLength = -1;
		for (int i = 0; (i < headerPieces.length) && (contentLength < 0); i++) {
			String[] thisHeaderPieceParts = headerPieces[i].split(":", 2);
			if (thisHeaderPieceParts.length == 2) {
				if (thisHeaderPieceParts[0].equals("Content-Length")) {
					try {
						contentLength = Integer.parseInt(thisHeaderPieceParts[1].trim());
					} catch (NumberFormatException nfEx) {
						contentLength = -1;
					}
				}
			}
		}

		if (contentLength < 0) {
			return null;
		}

		// now read the bytes of the content
		retContent = new byte[contentLength];
		int totalWant = contentLength;
		int totalRead = 0;
		while (totalRead < contentLength) {
			try {
				int numRead = in.read(retContent, totalRead, totalWant);
				if (numRead < 0) {
					return null;
				} else {
					totalRead += numRead;
					totalWant = contentLength - totalRead;
				} // end if (numRead < 0) / else
			} catch (EOFException eofEx) {
				// resize to what we have
				if (totalRead > 0) {
					byte[] newReturn = new byte[totalRead];
					System.arraycopy(retContent, 0, newReturn, 0, totalRead);
					return newReturn;
				} else {
					return null;
				}
			} // end try/catch (EOFException)
		} // end while (totalRead < contentLength)

		return retContent;
	}

	/**
	 * Reads in a WARC record from a data input stream
	 * 
	 * @param in
	 *            the input stream
	 * @return a WARC record (or null if eof)
	 * @throws java.io.IOException
	 */
	public static ClueWarcRecord readNextWarcRecord(DataInputStream in) throws IOException {
		StringBuffer recordHeader = new StringBuffer();
		byte[] recordContent = readNextRecord(in, recordHeader);
		if (recordContent == null) {
			return null;
		}

		// extract out our header information
		String thisHeaderString = recordHeader.toString();
		String[] headerLines = thisHeaderString.split(NEWLINE);

		ClueWarcRecord retRecord = new ClueWarcRecord();
		for (int i = 0; i < headerLines.length; i++) {
			String[] pieces = headerLines[i].split(":", 2);
			if (pieces.length != 2) {
				retRecord.addHeaderMetadata(pieces[0], "");
				continue;
			}
			String thisKey = pieces[0].trim();
			String thisValue = pieces[1].trim();

			// check for known keys
			if (thisKey.equals("WARC-Type")) {
				retRecord.setWarcRecordType(thisValue);
			} else if (thisKey.equals("WARC-Date")) {
				retRecord.setWarcDate(thisValue);
			} else if (thisKey.equals("WARC-Record-ID")) {
				retRecord.setWarcUUID(thisValue);
			} else if (thisKey.equals("Content-Type")) {
				retRecord.setWarcContentType(thisValue);
			} else {
				retRecord.addHeaderMetadata(thisKey, thisValue);
			}
		}

		// set the content
		retRecord.setContent(recordContent);

		previousTrecid = retRecord.getDocid();
		return retRecord;
	}

	private static String previousTrecid;

	/**
	 * Warc header class
	 */
	public class WarcHeader {
		public String contentType = "";
		public String UUID = "";
		public String dateString = "";
		public String recordType = "";
		public HashMap<String, String> metadata = new HashMap<String, String>();
		public int contentLength = 0;

		/**
		 * Default constructor
		 */
		public WarcHeader() {
		}

		/**
		 * Copy Constructor
		 * 
		 * @param o
		 *            other WARC header
		 */
		public WarcHeader(WarcHeader o) {
			this.contentType = o.contentType;
			this.UUID = o.UUID;
			this.dateString = o.dateString;
			this.recordType = o.recordType;
			this.metadata.putAll(o.metadata);
			this.contentLength = o.contentLength;
		}

		/**
		 * Serialization output
		 * 
		 * @param out
		 *            the data output stream
		 * @throws java.io.IOException
		 */
		public void write(DataOutput out) throws IOException {
			out.writeUTF(contentType);
			out.writeUTF(UUID);
			out.writeUTF(dateString);
			out.writeUTF(recordType);
			out.writeInt(metadata.size());
			Iterator<Entry<String, String>> metadataIterator = metadata.entrySet().iterator();
			while (metadataIterator.hasNext()) {
				Entry<String, String> thisEntry = metadataIterator.next();
				out.writeUTF(thisEntry.getKey());
				out.writeUTF(thisEntry.getValue());
			}
			out.writeInt(contentLength);
		}

		/**
		 * Serialization input
		 * 
		 * @param in
		 *            the data input stream
		 * @throws java.io.IOException
		 */
		public void readFields(DataInput in) throws IOException {
			contentType = in.readUTF();
			UUID = in.readUTF();
			dateString = in.readUTF();
			recordType = in.readUTF();
			metadata.clear();
			int numMetaItems = in.readInt();
			for (int i = 0; i < numMetaItems; i++) {
				String thisKey = in.readUTF();
				String thisValue = in.readUTF();
				metadata.put(thisKey, thisValue);
			}
			contentLength = in.readInt();
		}

		@Override
		public String toString() {
			StringBuffer retBuffer = new StringBuffer();

			retBuffer.append(WARC_VERSION);
			retBuffer.append(NEWLINE);

			retBuffer.append("WARC-Type: " + recordType + NEWLINE);
			retBuffer.append("WARC-Date: " + dateString + NEWLINE);

			retBuffer.append("WARC-Record-ID: " + UUID + NEWLINE);
			Iterator<Entry<String, String>> metadataIterator = metadata.entrySet().iterator();
			while (metadataIterator.hasNext()) {
				Entry<String, String> thisEntry = metadataIterator.next();
				retBuffer.append(thisEntry.getKey());
				retBuffer.append(": ");
				retBuffer.append(thisEntry.getValue());
				retBuffer.append(NEWLINE);
			}

			retBuffer.append("Content-Type: " + contentType + NEWLINE);
			retBuffer.append("Content-Length: " + contentLength + NEWLINE);

			return retBuffer.toString();
		}
	}

	private WarcHeader warcHeader = new WarcHeader();
	private byte[] warcContent = null;
	private String warcFilePath = "";

	/**
	 * Default Constructor
	 */
	public ClueWarcRecord() {
	}

	/**
	 * Copy Constructor
	 * 
	 * @param o
	 */
	public ClueWarcRecord(ClueWarcRecord o) {
		this.warcHeader = new WarcHeader(o.warcHeader);
		this.warcContent = o.warcContent;
	}

	/**
	 * Retrieves the total record length (header and content)
	 * 
	 * @return total record length
	 */
	public int getTotalRecordLength() {
		int headerLength = warcHeader.toString().length();
		return (headerLength + warcContent.length);
	}

	/**
	 * Sets the record content (copy)
	 * 
	 * @param o
	 *            record to copy from
	 */
	public void set(ClueWarcRecord o) {
		this.warcHeader = new WarcHeader(o.warcHeader);
		this.warcContent = o.warcContent;
		this.mCachedCleanedContent = null;
	}

	/**
	 * Gets the file path from this WARC file (if set)
	 * 
	 * @return
	 */
	public String getWarcFilePath() {
		return warcFilePath;
	}

	/**
	 * Sets the warc file path (optional - for use with getWarcFilePath)
	 * 
	 * @param path
	 */
	public void setWarcFilePath(String path) {
		warcFilePath = path;
	}

	/**
	 * Sets the record type string
	 * 
	 * @param recordType
	 */
	public void setWarcRecordType(String recordType) {
		warcHeader.recordType = recordType;
	}

	/**
	 * Sets the content type string
	 * 
	 * @param contentType
	 */
	public void setWarcContentType(String contentType) {
		warcHeader.contentType = contentType;
	}

	/**
	 * Sets the WARC header date string
	 * 
	 * @param dateString
	 */
	public void setWarcDate(String dateString) {
		warcHeader.dateString = dateString;
	}

	/**
	 * Sets the WARC uuid string
	 * 
	 * @param UUID
	 */
	public void setWarcUUID(String UUID) {
		warcHeader.UUID = UUID;
	}

	/**
	 * Adds a key/value pair to a WARC header. This is needed to filter out
	 * known keys
	 * 
	 * @param key
	 * @param value
	 */
	public void addHeaderMetadata(String key, String value) {
		// don't allow addition of known keys
		if (key.equals("WARC-Type")) {
			return;
		}
		if (key.equals("WARC-Date")) {
			return;
		}
		if (key.equals("WARC-Record-ID")) {
			return;
		}
		if (key.equals("Content-Type")) {
			return;
		}
		if (key.equals("Content-Length")) {
			return;
		}

		warcHeader.metadata.put(key, value);
	}

	/**
	 * Clears all metadata items from a header
	 */
	public void clearHeaderMetadata() {
		warcHeader.metadata.clear();
	}

	/**
	 * Gets the set of metadata items from the header
	 * 
	 * @return
	 */
	public Set<Entry<String, String>> getHeaderMetadata() {
		return warcHeader.metadata.entrySet();
	}

	/**
	 * Gets a value for a specific header metadata key
	 * 
	 * @param key
	 * @return
	 */
	public String getHeaderMetadataItem(String key) {
		return warcHeader.metadata.get(key);
	}

	/**
	 * Sets the byte content for this record
	 * 
	 * @param content
	 */
	public void setContent(byte[] content) {
		warcContent = content;
		warcHeader.contentLength = content.length;
	}

	/**
	 * Sets the byte content for this record
	 * 
	 * @param content
	 */
	public void setContent(String content) {
		setContent(content.getBytes());
	}

	/**
	 * Restrieves the byte content for this record
	 * 
	 * @return
	 */
	public byte[] getByteContent() {
		return warcContent;
	}

	/**
	 * Retrieves the bytes content as a UTF-8 string
	 * 
	 * @return
	 */
	public String getContentUTF8() {
		String retString = null;
		try {
			retString = new String(warcContent, "UTF-8");
		} catch (UnsupportedEncodingException ex) {
			retString = new String(warcContent);
		}
		return retString;
	}

	/**
	 * Gets the header record type string
	 * 
	 * @return
	 */
	public String getHeaderRecordType() {
		return warcHeader.recordType;
	}

	@Override
	public String toString() {
		StringBuffer retBuffer = new StringBuffer();
		retBuffer.append(warcHeader.toString());
		retBuffer.append(NEWLINE);
		retBuffer.append(warcContent);
		return retBuffer.toString();
	}

	/**
	 * Gets the WARC header as a string
	 * 
	 * @return
	 */
	public String getHeaderString() {
		return warcHeader.toString();
	}

	/**
	 * Serialization output
	 * 
	 * @param out
	 * @throws java.io.IOException
	 */
	public void write(DataOutput out) throws IOException {
		warcHeader.write(out);
		out.write(warcContent);

		if (mCachedCleanedContent == null)
			mCachedCleanedContent = getContent();

		byte[] bytes = mCachedCleanedContent.getBytes();
		// sLogger.info("writing: " + this.getDocid() + ", " + bytes.length);

		out.writeInt(bytes.length);
		out.write(bytes);
	}

	/**
	 * Serialization input
	 * 
	 * @param in
	 * @throws java.io.IOException
	 */
	public void readFields(DataInput in) throws IOException {
		warcHeader.readFields(in);
		int contentLengthBytes = warcHeader.contentLength;
		warcContent = new byte[contentLengthBytes];
		in.readFully(warcContent);

		int cz = in.readInt();
		byte[] bytes = new byte[cz];
		in.readFully(bytes);
		mCachedCleanedContent = new String(bytes);

	}

	public String getDocid() {
		return getHeaderMetadataItem("WARC-TREC-ID");
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

	public String getContent() {
		if (mCachedCleanedContent != null)
			return mCachedCleanedContent;

		// return getContentUTF8();
		Matcher m = PATTERN_BODY.matcher(getContentUTF8());

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

	private String mCachedCleanedContent = null;

	public String getHTML() {
		String s = getContentUTF8();
		return getContentUTF8().substring(s.indexOf("\n\n"));
	}
}
