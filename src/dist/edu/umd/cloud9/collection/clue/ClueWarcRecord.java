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

	static Set<String> errors2 = new HashSet<String>();
	static {
		// part 4
		errors2.add("clueweb09-en0044-01-04501");
		
		// part 5
		errors2.add("clueweb09-en0059-46-06368");

		// part 9
		errors2.add("clueweb09-en0117-48-12547");

		// part 10
		errors2.add("clueweb09-en0126-33-37391");
		errors2.add("clueweb09-en0126-88-10049");
	}
	
	static Set<String> errors = new HashSet<String>();
	static {
		// part 1
		errors.add("clueweb09-en0001-41-14941");
		errors.add("clueweb09-en0003-88-28589");
		errors.add("clueweb09-en0005-46-08669");
		errors.add("clueweb09-en0007-31-02866");
		errors.add("clueweb09-en0007-91-00093");
		errors.add("clueweb09-en0007-93-23823");
		errors.add("clueweb09-en0009-60-41300");
		errors.add("clueweb09-en0009-60-41302");

		// part 2
		errors.add("clueweb09-en0016-28-15816");
		errors.add("clueweb09-en0018-85-25777");
		errors.add("clueweb09-en0019-97-01055");
		errors.add("clueweb09-en0020-37-41738");
		errors.add("clueweb09-en0021-24-31539");
		errors.add("clueweb09-en0021-24-31541");
		errors.add("clueweb09-en0021-24-31552");
		errors.add("clueweb09-en0021-24-31554");
		errors.add("clueweb09-en0021-24-31572");
		errors.add("clueweb09-en0021-41-00137");
		errors.add("clueweb09-en0023-97-13993");
		errors.add("clueweb09-en0023-97-14978");

		// part 3
		errors.add("clueweb09-en0030-15-04127");
		errors.add("clueweb09-en0030-62-18205");
		errors.add("clueweb09-en0030-62-18209");
		errors.add("clueweb09-en0032-50-32906");
		errors.add("clueweb09-en0032-50-32969");
		errors.add("clueweb09-en0032-66-26110");
		errors.add("clueweb09-en0032-80-27292");
		errors.add("clueweb09-en0035-66-28044");
		errors.add("clueweb09-en0035-66-28074");
		errors.add("clueweb09-en0035-66-28222");
		errors.add("clueweb09-en0035-80-26647");
		errors.add("clueweb09-en0035-80-26651");
		errors.add("clueweb09-en0035-80-26663");
		errors.add("clueweb09-en0035-80-26664");
		errors.add("clueweb09-en0035-91-11777");
		errors.add("clueweb09-en0035-91-11852");

		// part 4
		errors.add("clueweb09-en0041-70-34116"); //
		errors.add("clueweb09-en0042-40-34405"); //
		errors.add("clueweb09-en0043-73-09059"); //
		errors.add("clueweb09-en0044-28-14850"); //
		errors.add("clueweb09-en0045-40-13370"); //
		errors.add("clueweb09-en0045-40-13378"); //
		errors.add("clueweb09-en0047-53-04057"); //
		errors.add("clueweb09-en0047-79-14724"); //
		errors.add("clueweb09-en0049-08-27467"); //
		errors.add("clueweb09-en0049-08-27469"); //
		errors.add("clueweb09-en0049-52-17127"); //
		errors.add("clueweb09-en0049-56-38703"); //
		errors.add("clueweb09-en0049-56-38706"); //
		errors.add("clueweb09-en0049-56-38712"); //
		errors.add("clueweb09-en0049-56-38713"); //
		errors.add("clueweb09-en0049-56-38714"); //
		errors.add("clueweb09-en0049-56-38715"); //
		errors.add("clueweb09-en0051-79-15041"); //
		errors.add("clueweb09-en0052-01-00911"); //
		errors.add("clueweb09-en0052-01-00935"); //

		// part 5
		errors.add("clueweb09-en0058-26-24946"); //
		errors.add("clueweb09-en0060-02-11350"); //
		errors.add("clueweb09-en0060-02-11352"); //
		errors.add("clueweb09-en0061-83-10291"); //
		errors.add("clueweb09-en0063-51-34432"); //
		errors.add("clueweb09-en0063-51-34436"); //
		errors.add("clueweb09-en0063-51-34437"); //
		errors.add("clueweb09-en0063-51-34438"); //
		errors.add("clueweb09-en0063-51-34439"); //
		errors.add("clueweb09-en0063-78-17592"); //
		errors.add("clueweb09-en0063-78-17608"); //
		errors.add("clueweb09-en0063-78-17940"); //
		errors.add("clueweb09-en0065-67-26241"); //

		// part 6
		errors.add("clueweb09-en0073-24-22329");
		errors.add("clueweb09-en0073-24-22339");
		errors.add("clueweb09-en0073-24-22352");
		errors.add("clueweb09-en0074-31-09086");
		errors.add("clueweb09-en0074-84-02250");
		errors.add("clueweb09-en0077-31-34097");
		errors.add("clueweb09-en0078-35-17333");
		errors.add("clueweb09-en0078-65-13858");
		errors.add("clueweb09-en0078-91-13081");
		errors.add("clueweb09-en0079-26-06558");
		
		// part 7
		errors.add("clueweb09-en0084-40-00165");
		errors.add("clueweb09-en0084-40-00172");
		errors.add("clueweb09-en0086-99-19332");
		errors.add("clueweb09-en0090-01-05591");
		errors.add("clueweb09-en0091-76-28810");
		errors.add("clueweb09-en0091-76-28814");
		errors.add("clueweb09-en0091-76-28949");
		errors.add("clueweb09-en0092-51-03618");
		errors.add("clueweb09-en0092-51-03620");
		errors.add("clueweb09-en0092-51-03621");
		errors.add("clueweb09-en0092-51-03622");
		errors.add("clueweb09-en0092-51-03623");
		errors.add("clueweb09-en0092-51-03624");
		errors.add("clueweb09-en0092-51-03625");

		// part 8
		errors.add("clueweb09-en0100-01-00311");
		errors.add("clueweb09-en0100-08-28250");
		errors.add("clueweb09-en0101-12-07414");
		errors.add("clueweb09-en0101-95-20336");
		errors.add("clueweb09-en0105-93-12436");
		errors.add("clueweb09-en0105-93-12438");
		errors.add("clueweb09-en0105-93-12439");
		errors.add("clueweb09-en0105-93-12440");
		errors.add("clueweb09-en0105-93-12441");
		errors.add("clueweb09-en0105-93-12442");
		errors.add("clueweb09-en0105-93-12443");
		errors.add("clueweb09-en0105-93-12444");
		errors.add("clueweb09-en0105-93-12445");
		errors.add("clueweb09-en0105-93-12446");
		errors.add("clueweb09-en0107-58-21769");
		errors.add("clueweb09-en0109-88-38443");

		// part 9
		errors.add("clueweb09-en0110-80-12838"); //
		errors.add("clueweb09-en0112-59-06118");
		errors.add("clueweb09-en0113-08-03899"); //
		errors.add("clueweb09-en0115-33-02340"); //
		errors.add("clueweb09-en0117-48-12547");
		errors.add("clueweb09-en0117-89-14585"); //
		errors.add("clueweb09-en0119-41-43728"); //
		errors.add("clueweb09-en0119-41-43730"); //
		errors.add("clueweb09-en0119-41-43731"); //
		errors.add("clueweb09-en0119-41-43732"); //
		errors.add("clueweb09-en0119-41-43733"); //
		errors.add("clueweb09-en0121-89-41649"); //

		// part 10
		//errors.add("clueweb09-en0126-33-37391");
		errors.add("clueweb09-en0126-37-13778"); //
		errors.add("clueweb09-en0126-87-37931"); //
		//errors.add("clueweb09-en0126-88-10049");
		errors.add("clueweb09-en0126-92-38225"); //
		errors.add("clueweb09-en0127-16-00160"); //
		errors.add("clueweb09-en0127-29-01098"); //
		errors.add("clueweb09-en0129-86-14453"); //
		errors.add("clueweb09-en0130-16-28129"); //
		errors.add("clueweb09-en0130-54-25838"); //
		errors.add("clueweb09-en0130-58-50061"); //
		errors.add("clueweb09-en0130-58-50063"); //
		errors.add("clueweb09-en0130-58-50065"); //
		errors.add("clueweb09-en0130-92-39888"); //
		errors.add("clueweb09-en0130-92-39999"); //
		errors.add("clueweb09-en0130-92-40090"); //
		errors.add("clueweb09-en0130-92-40700"); //
		errors.add("clueweb09-en0131-66-26013"); //
		errors.add("clueweb09-en0131-66-26014"); //
		errors.add("clueweb09-en0131-66-26015"); //
		errors.add("clueweb09-en0131-66-26016"); //
		errors.add("clueweb09-en0131-66-26017"); //
		errors.add("clueweb09-en0132-87-39267"); //
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
		boolean ignoreSecondEmptyLine = false;
		boolean ignoreThirdEmptyLine = false;
		
		if (previousTrecid != null && errors.contains(previousTrecid)) {
			sLogger.info("Special handling of errors following record " + previousTrecid + " (case 1)");
			ignoreFirstEmptyLine = true;
		}

		if (previousTrecid != null && errors2.contains(previousTrecid)) {
			sLogger.info("Special handling of errors following record " + previousTrecid + " (case 2)");
			ignoreFirstEmptyLine = true;
			ignoreSecondEmptyLine = true;
		}

		if (previousTrecid != null && previousTrecid.equals("clueweb09-en0112-59-06118")) {
			sLogger.info("Special handling of errors following record " + previousTrecid + " (case 3)");
			ignoreFirstEmptyLine = true;
			ignoreSecondEmptyLine = true;
			ignoreThirdEmptyLine = true;
		}

		// then read to the first newline
		// get the content length and set our retContent
		while (inHeader && ((line = readLineFromInputStream(in)) != null)) {
			//System.out.println(line);
			if (line.trim().length() == 0 && ignoreFirstEmptyLine) {
				ignoreFirstEmptyLine = false;
				continue;
			}

			if (line.trim().length() == 0 && ignoreSecondEmptyLine) {
				ignoreSecondEmptyLine = false;
				continue;
			}

			if (line.trim().length() == 0 && ignoreThirdEmptyLine) {
				ignoreThirdEmptyLine = false;
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
