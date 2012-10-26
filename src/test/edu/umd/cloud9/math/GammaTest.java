package edu.umd.cloud9.math;

import static org.junit.Assert.assertEquals;
import junit.framework.JUnit4TestAdapter;

import org.junit.Test;

public class GammaTest {
  public static double PRECISION_3 = 1e-3f;
  public static double PRECISION_6 = 1e-6f;
  public static double PRECISION_9 = 1e-9f;
  public static double PRECISION_12 = 1e-12f;

  @Test
  public void testDigamma() {
    assertEquals(Gamma.digamma(1000000), 13.81551005796419, PRECISION_12);
    assertEquals(Gamma.digamma(100000), 11.512920464961896, PRECISION_12);
    assertEquals(Gamma.digamma(10000), 9.21029037114285, PRECISION_12);

    assertEquals(Gamma.digamma(1000), 6.907255195648812, PRECISION_12);
    assertEquals(Gamma.digamma(100), 4.600161852738087, PRECISION_12);
    assertEquals(Gamma.digamma(10), 2.2517525890667214, PRECISION_12);

    assertEquals(Gamma.digamma(1), -0.5772156649015328, PRECISION_12);
    assertEquals(Gamma.digamma(0.1), -10.42375494041107, PRECISION_12);
    assertEquals(Gamma.digamma(0.01), -100.56088545786886, PRECISION_12);

    // precision drops down accordingly when computing digamma function for small value
    assertEquals(Gamma.digamma(0.001), -1000.5755719318336, PRECISION_9);
    assertEquals(Gamma.digamma(0.0001), -10000.57705117741, PRECISION_6);
    assertEquals(Gamma.digamma(0.00001), -100000.57719922789, PRECISION_6);

    assertEquals(Gamma.digamma(-0.001), 999.4211381980015, PRECISION_9);

    assertEquals(Gamma.digamma(-0.01), 99.4062136959443, PRECISION_12);
    assertEquals(Gamma.digamma(-0.1), 9.245073050052941, PRECISION_12);
  }

  @Test
  public void testTrigamma() {
    assertEquals(Gamma.trigamma(1000000), 1.0000005000001667E-6, PRECISION_12);
    assertEquals(Gamma.trigamma(100000), 1.0000050000166667E-5, PRECISION_12);
    assertEquals(Gamma.trigamma(10000), 1.0000500016666666E-4, PRECISION_12);

    assertEquals(Gamma.trigamma(1000), 0.0010005001666666333, PRECISION_12);
    assertEquals(Gamma.trigamma(100), 0.010050166663333571, PRECISION_12);
    assertEquals(Gamma.trigamma(10), 0.10516633568168571, PRECISION_12);

    // precision drops down accordingly when computing digamma function for small value
    assertEquals(Gamma.trigamma(1), 1.6449340668482264, PRECISION_9);
    assertEquals(Gamma.trigamma(0.1), 101.4332991507927, PRECISION_9);
    assertEquals(Gamma.trigamma(0.01), 10001.62121352835, PRECISION_9);
    assertEquals(Gamma.trigamma(0.001), 1000001.6425332422, PRECISION_6);

    assertEquals(Gamma.trigamma(-0.001), 1000001.6473416518, PRECISION_6);
    assertEquals(Gamma.trigamma(-0.01), 10001.669304101055, PRECISION_9);
    assertEquals(Gamma.trigamma(-0.1), 101.92253995947704, PRECISION_9);
  }

  @Test
  public void testLogGamma() {
    assertEquals(edu.umd.cloud9.math.Gamma.lgamma(1000000), cern.jet.stat.Gamma.logGamma(1000000),
        PRECISION_12);
    assertEquals(edu.umd.cloud9.math.Gamma.lgamma(100000), cern.jet.stat.Gamma.logGamma(100000),
        PRECISION_12);
    assertEquals(edu.umd.cloud9.math.Gamma.lgamma(10000), cern.jet.stat.Gamma.logGamma(10000),
        PRECISION_9);

    assertEquals(edu.umd.cloud9.math.Gamma.lgamma(1000), cern.jet.stat.Gamma.logGamma(1000),
        PRECISION_12);
    assertEquals(edu.umd.cloud9.math.Gamma.lgamma(100), cern.jet.stat.Gamma.logGamma(100),
        PRECISION_12);
    assertEquals(edu.umd.cloud9.math.Gamma.lgamma(10), cern.jet.stat.Gamma.logGamma(10),
        PRECISION_12);

    // precision drops down accordingly when computing digamma function for small value
    assertEquals(edu.umd.cloud9.math.Gamma.lgamma(1), cern.jet.stat.Gamma.logGamma(1), PRECISION_9);
    assertEquals(edu.umd.cloud9.math.Gamma.lgamma(0.1), cern.jet.stat.Gamma.logGamma(0.1),
        PRECISION_9);
    assertEquals(edu.umd.cloud9.math.Gamma.lgamma(0.01), cern.jet.stat.Gamma.logGamma(0.01),
        PRECISION_9);
    assertEquals(edu.umd.cloud9.math.Gamma.lgamma(0.001), cern.jet.stat.Gamma.logGamma(0.001),
        PRECISION_6);
  }

  @Test
  public void logGammaSpeedTest() {
    long time = System.currentTimeMillis();
    for (int i = 0; i < 99999; i++) {
      edu.umd.cloud9.math.Gamma.lgamma(i + 1);
    }
    System.out.println(System.currentTimeMillis() - time);

    time = System.currentTimeMillis();
    for (int i = 0; i < 99999; i++) {
      edu.umd.cloud9.math.Gamma.lanczosLGamma9(i + 1);
    }
    System.out.println(System.currentTimeMillis() - time);

    time = System.currentTimeMillis();
    for (int i = 0; i < 99999; i++) {
      edu.umd.cloud9.math.Gamma.lanczosLGamma15(i + 1);
    }
    System.out.println(System.currentTimeMillis() - time);

    time = System.currentTimeMillis();
    for (int i = 0; i < 99999; i++) {
      edu.umd.cloud9.math.Gamma.stirlingLGamma(i + 1);
    }
    System.out.println(System.currentTimeMillis() - time);

    // time = System.currentTimeMillis();
    // for (int i = 0; i < 99999; i++) {
    // cc.mrlda.util.Gamma.logGamma(0.5);
    // }
    // System.out.println(System.currentTimeMillis() - time);

    time = System.currentTimeMillis();
    for (int i = 0; i < 99999; i++) {
      cern.jet.stat.Gamma.logGamma(i + 1);
    }
    System.out.println(System.currentTimeMillis() - time);
  }

  public static junit.framework.Test suite() {
    return new JUnit4TestAdapter(GammaTest.class);
  }
}