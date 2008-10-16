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
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class Int3WritableTest {

	@Test
	public void testBasic() throws IOException {

		int[] intArray = { 0, 1, 2, 3554, 4353, 8320, 16777214, 16777215 };

		for (int i : intArray) {
			Int3Writable origInt3 = new Int3Writable(i);

			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(bytesOut);

			origInt3.write(dataOut);

			Int3Writable newInt3 = new Int3Writable();

			newInt3
					.readFields(new DataInputStream(
							new ByteArrayInputStream(bytesOut.toByteArray())));

			assertEquals(i, newInt3.get());
		}
	}

	@Test
	public void testOverflow() throws IOException {

		int[] intArray = { 16777216, 16777217 };

		for (int i : intArray) {
			Int3Writable origInt3 = new Int3Writable(i);

			ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(bytesOut);

			origInt3.write(dataOut);

			Int3Writable newInt3 = new Int3Writable();

			newInt3
					.readFields(new DataInputStream(
							new ByteArrayInputStream(bytesOut.toByteArray())));

			assertEquals(i - 16777216, newInt3.get());
		}
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(Int3WritableTest.class);
	}

}
