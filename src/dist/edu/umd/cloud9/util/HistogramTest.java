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

package edu.umd.cloud9.util;

import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class HistogramTest {

	@Test
	public void test1() {
		Histogram<String> h = new Histogram<String>();

		h.count("yes");
		h.count("yes");
		h.count("yes");
		h.count("no");
		h.count("no");
		h.count("maybe");
		h.count("so");

		assertEquals(3, h.getCount("yes"));
		assertEquals(2, h.getCount("no"));
		assertEquals(1, h.getCount("maybe"));
		assertEquals(1, h.getCount("so"));
		
		MapKI.Entry<String>[] arr = h.getEntriesSortedByValue();

		assertEquals("yes", arr[0].getKey());
		assertEquals(3, arr[0].getValue());

		assertEquals("no", arr[1].getKey());
		assertEquals(2, arr[1].getValue());

		assertEquals("maybe", arr[2].getKey());
		assertEquals(1, arr[2].getValue());

		assertEquals("so", arr[3].getKey());
		assertEquals(1, arr[3].getValue());

		assertEquals(7, h.getTotalCount());

		h.clear();

		assertEquals(0, h.getTotalCount());

		assertEquals(0, h.getCount("yes"));
		assertEquals(0, h.getCount("no"));
		assertEquals(0, h.getCount("maybe"));
		assertEquals(0, h.getCount("so"));

		h.count("yes");
		h.count("yes");
		h.count("no");

		assertEquals(2, h.getCount("yes"));
		assertEquals(1, h.getCount("no"));

	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(HistogramTest.class);
	}

}
