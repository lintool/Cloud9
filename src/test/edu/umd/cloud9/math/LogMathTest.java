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

package edu.umd.cloud9.math;

import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

import org.junit.Test;


public class LogMathTest {
  public static double PRECISION_12 = 1e-12;

  @Test
  public void testAdd() {
    assertEquals(LogMath.add(Math.log(1), Math.log(1)), Math.log(2), PRECISION_12);
    assertEquals(LogMath.add(Math.log(0.1), Math.log(0.1)), Math.log(0.2), PRECISION_12);
    assertEquals(LogMath.add(Math.log(10), Math.log(10)), Math.log(20), PRECISION_12);

    assertEquals(LogMath.add(Math.log(1), Math.log(0.1)), Math.log(1.1), PRECISION_12);
    assertEquals(LogMath.add(Math.log(0.1), Math.log(1)), Math.log(1.1), PRECISION_12);

    assertEquals(LogMath.add(Math.log(1), Math.log(10)), Math.log(11), PRECISION_12);
    assertEquals(LogMath.add(Math.log(10), Math.log(1)), Math.log(11), PRECISION_12);

    assertEquals(LogMath.add(Math.log(0.1), Math.log(10)), Math.log(10.1), PRECISION_12);
    assertEquals(LogMath.add(Math.log(10), Math.log(0.1)), Math.log(10.1), PRECISION_12);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(LogMathTest.class);
  }
}