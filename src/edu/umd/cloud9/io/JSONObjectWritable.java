package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

public class JSONObjectWritable extends JSONObject implements Writable {

	/**
	 * Deserializes the JSON object.
	 * 
	 * @param in
	 *            source for raw byte representation
	 */
	@SuppressWarnings("unchecked")
	public void readFields(DataInput in) throws IOException {
		String s = in.readLine();

		// following block of code copied from JSONObject
		try {
			JSONTokener x = new JSONTokener(s);

			char c;
			String key;

			if (x.nextClean() != '{') {
				throw x.syntaxError("A JSONObject text must begin with '{'");
			}
			for (;;) {
				c = x.nextClean();
				switch (c) {
				case 0:
					throw x.syntaxError("A JSONObject text must end with '}'");
				case '}':
					return;
				default:
					x.back();
					key = x.nextValue().toString();
				}

				/*
				 * The key is followed by ':'. We will also tolerate '=' or
				 * '=>'.
				 */

				c = x.nextClean();
				if (c == '=') {
					if (x.next() != '>') {
						x.back();
					}
				} else if (c != ':') {
					throw x.syntaxError("Expected a ':' after a key");
				}
				put(key, x.nextValue());

				/*
				 * Pairs are separated by ','. We will also tolerate ';'.
				 */

				switch (x.nextClean()) {
				case ';':
				case ',':
					if (x.nextClean() == '}') {
						return;
					}
					x.back();
					break;
				case '}':
					return;
				default:
					throw x.syntaxError("Expected a ',' or '}'");
				}
			}
		} catch (JSONException e) {
			throw new IOException();
		}
	}

	/**
	 * Serializes this JSON object.
	 * 
	 * @param out
	 *            where to write the raw byte representation
	 */
	@SuppressWarnings("unchecked")
	public void write(DataOutput out) throws IOException {
		out.writeBytes(this.toString());
	}

}
