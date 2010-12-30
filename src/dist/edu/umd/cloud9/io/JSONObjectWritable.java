/*
 * Cloud9: A MapReduce Library for Hadoop
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package edu.umd.cloud9.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * Writable representing a JSON Object.
 *
 * @author Jimmy Lin
 */
public class JSONObjectWritable extends JSONObject implements Writable {

	/**
	 * Creates an empty JSONWritableObject.
	 */
	public JSONObjectWritable() {
		super();
	}

	/**
	 * Creates a JSONWritableObject with an initial value.
	 */
	public JSONObjectWritable(String s) throws JSONException {
		super(s);
	}

	/**
	 * Deserializes the JSON object.
	 *
	 * @param in source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		super.map.clear();

		int cnt = in.readInt();
		byte[] buf = new byte[cnt];
		in.readFully(buf);
		String s = new String(buf);

		readJSONObject(s);
	}

	/**
	 * Deserializes a JSON object from a string representation.
	 *
	 * @param s string representation of the JSON object
	 */
	public void readJSONObject(String s) {
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
			throw new RuntimeException("Error: invalid JSON!");
		}
	}

	/**
	 * Serializes this JSON object.
	 *
	 * @param out where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		byte[] buf = this.toString().getBytes();
		out.writeInt(buf.length);
		out.write(buf);
	}

	public boolean getBooleanUnchecked(String key) throws JSONException {
		return (Boolean) super.map.get(key);
	}

	public double getDoubleUnchecked(String key) throws JSONException {
		return (Double) super.map.get(key);
	}

	public int getIntUnchecked(String key) throws JSONException {
		return (Integer) super.map.get(key);
	}

	public long getLongUnchecked(String key) throws JSONException {
		return (Long) super.map.get(key);
	}

	public String getStringUnchecked(String key) throws JSONException {
		return (String) super.map.get(key);
	}

	public void clear() {
		super.map.clear();
	}
}
