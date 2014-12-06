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

package edu.umd.cloud9.util.pair;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

import com.google.common.collect.Lists;

import edu.umd.cloud9.util.pair.PairOfObjectInt;

public class PairOfObjectIntTest {

	@Test
	public void test1() {
		PairOfObjectInt<String> pair = new PairOfObjectInt<String>("a", 1);
		assertEquals("a", pair.getLeftElement());
		assertEquals(1, pair.getRightElement());

		pair.setLeftElement("b");
		assertEquals("b", pair.getLeftElement());

		pair.setRightElement(5);
		assertEquals(5, pair.getRightElement());

		pair.set("foo", -1);
		assertEquals("foo", pair.getLeftElement());
		assertEquals(-1, pair.getRightElement());
	}

	@Test
	public void testIterable() {
		List<PairOfObjectInt<String>> list = Lists.newArrayList();

		list.add(new PairOfObjectInt<String>("f", 9));
		list.add(new PairOfObjectInt<String>("a", 1));
		list.add(new PairOfObjectInt<String>("b", 4));
		list.add(new PairOfObjectInt<String>("b", 2));
		list.add(new PairOfObjectInt<String>("c", 2));
		list.add(new PairOfObjectInt<String>("a", 3));

		assertEquals(6, list.size());
		Collections.sort(list);

		assertEquals("a", list.get(0).getLeftElement());
		assertEquals(1, list.get(0).getRightElement());
		assertEquals("a", list.get(1).getLeftElement());
		assertEquals(3, list.get(1).getRightElement());
		assertEquals("b", list.get(2).getLeftElement());
		assertEquals(2, list.get(2).getRightElement());
		assertEquals("b", list.get(3).getLeftElement());
		assertEquals(4, list.get(3).getRightElement());
		assertEquals("c", list.get(4).getLeftElement());
		assertEquals(2, list.get(4).getRightElement());
		assertEquals("f", list.get(5).getLeftElement());
		assertEquals(9, list.get(5).getRightElement());
	}

	public static junit.framework.Test suite() {
		return new JUnit4TestAdapter(PairOfObjectIntTest.class);
	}
}
