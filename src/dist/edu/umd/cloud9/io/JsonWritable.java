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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Writable representing a JSON object.
 *
 * @author Jimmy Lin
 */
public class JsonWritable implements Writable {
  private static final Gson GSON = new Gson();
  private static final JsonParser PARSER = new JsonParser();
  private JsonObject json;

	/**
	 * Creates an empty {@code JsonWritable}.
	 */
	public JsonWritable() {
	  json = new JsonObject();
	}

	/**
	 * Creates a {@code JsonWritable} with an initial value.
	 */
	public JsonWritable(String s) {
		json = (JsonObject) PARSER.parse(s);
	}

	/**
	 * Deserializes a {@code JsonWritable} object.
	 *
	 * @param in source for raw byte representation
	 */
	public void readFields(DataInput in) throws IOException {
		int cnt = in.readInt();
		byte[] buf = new byte[cnt];
		in.readFully(buf);
		json = (JsonObject) PARSER.parse(new String(buf, "UTF-8"));
	}

	/**
	 * Serializes this object.
	 *
	 * @param out where to write the raw byte representation
	 */
	public void write(DataOutput out) throws IOException {
		byte[] buf = GSON.toJson(json).getBytes();
		out.writeInt(buf.length);
		out.write(buf);
	}

  /**
   * Returns the serialized representation of this object as a byte array.
   * 
   * @return byte array representing the serialized representation of this object
   * @throws IOException
   */
  public byte[] serialize() throws IOException {
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    write(dataOut);

    return bytesOut.toByteArray();
  }

	public JsonObject getJsonObject() {
	  return json;
	}

	@Override
	public String toString() {
	  return getJsonObject().toString();
	}

  public static JsonWritable create(DataInput in) throws IOException {
    JsonWritable json = new JsonWritable();
    json.readFields(in);

    return json;
  }

  public static JsonWritable create(byte[] bytes) throws IOException {
    return create(new DataInputStream(new ByteArrayInputStream(bytes)));
  }
}
