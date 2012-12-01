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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

public class JsonWritableTest {

	@Test
	public void testSerialize1() throws Exception {
		JsonWritable obj1 = new JsonWritable();
		obj1.getJsonObject().addProperty("JSON", "Hello, World!");

		byte[] bytes = obj1.serialize();
		JsonWritable obj2 = JsonWritable.create(bytes);

		assertEquals("{\"JSON\":\"Hello, World!\"}", obj2.getJsonObject().toString());
	}
	
	@Test
	public void testSerialize2() throws Exception {
		JsonWritable obj1 = new JsonWritable();
		obj1.getJsonObject().addProperty("JSON", "'tis");

    byte[] bytes = obj1.serialize();
    JsonWritable obj2 = JsonWritable.create(bytes);

    assertEquals("{\"JSON\":\"'tis\"}", obj2.getJsonObject().toString());
	}

	@Test
	public void testSerialize() throws Exception {
		JsonWritable w = new JsonWritable();
		JsonObject obj = w.getJsonObject();

		obj.addProperty("firstName", "John");
		obj.addProperty("lastName", "Smith");

		JsonObject address = new JsonObject();
		address.addProperty("streetAddress", "21 2nd Street");
		address.addProperty("city", "New York");
		address.addProperty("state", "NY");
		address.addProperty("postalCode", 10021);

		JsonArray phoneNumbers = new JsonArray();
		phoneNumbers.add(new JsonPrimitive("212 555-1234"));
		phoneNumbers.add(new JsonPrimitive("646 555-4567"));

		obj.add("address", address);
		obj.add("phoneNumbers", phoneNumbers);

    byte[] bytes = w.serialize();
    JsonWritable w2 = JsonWritable.create(bytes);

		String s = "{\"firstName\":\"John\",\"lastName\":\"Smith\"," +
		    "\"address\":{\"streetAddress\":\"21 2nd Street\",\"city\":\"New York\",\"state\":\"NY\",\"postalCode\":10021}," +
		    "\"phoneNumbers\":[\"212 555-1234\",\"646 555-4567\"]}";

		assertEquals(s, w2.getJsonObject().toString());

		JsonObject obj2 = w2.getJsonObject();
		assertEquals("John", obj2.get("firstName").getAsString());
		assertEquals("Smith", obj2.get("lastName").getAsString());
		assertEquals("New York", obj2.getAsJsonObject("address").get("city").getAsString());
		assertEquals(2, obj2.getAsJsonArray("phoneNumbers").size());
		assertEquals("212 555-1234",
		    ((JsonPrimitive) obj2.getAsJsonArray("phoneNumbers").get(0)).getAsString());
	}

	@Test
	public void testRewrite() throws Exception {
		String s1 = "{\"JSON\":\"Hello, World!\"}";
		String s2 = "{\"JSON\":\"Hello!\"}";

		JsonWritable obj = new JsonWritable(s1);
		assertEquals(obj.getJsonObject().toString(), s1);

    byte[] s2Bytes = s2.getBytes();
    ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(bytesOut);
    dataOut.writeInt(s2Bytes.length);
    dataOut.write(s2Bytes);

		DataInput dataIn = new DataInputStream(new ByteArrayInputStream(bytesOut.toByteArray()));
		obj.readFields(dataIn);

		assertEquals(obj.getJsonObject().toString(), s2);
	}
	
	@Test
	public void testOverwrite() throws Exception {
		JsonWritable obj = new JsonWritable();
		obj.getJsonObject().addProperty("field", "longer string");
		assertEquals("{\"field\":\"longer string\"}", obj.getJsonObject().toString());
		
		obj.getJsonObject().addProperty("field", "a");
		assertEquals("{\"field\":\"a\"}", obj.getJsonObject().toString());
	}
	
	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(JsonWritableTest.class);
	}

}
