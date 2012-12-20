package edu.umd.cloud9.math;

import com.google.common.base.Preconditions;

/*
 Author Mihai Preda, 2006. 
 The author disclaims copyright to this source code.

 The method lgamma() is adapted from FDLIBM 5.3 (http://www.netlib.org/fdlibm/), 
 which comes with this copyright notice:
 * ====================================================
 * Copyright (C) 1993 by Sun Microsystems, Inc. All rights reserved.
 *
 * Developed at SunSoft, a Sun Microsystems, Inc. business.
 * Permission to use, copy, modify, and distribute this
 * software is freely granted, provided that this notice 
 * is preserved.
 * ====================================================

 The Lanczos and Stirling approximations are based on:
 http://en.wikipedia.org/wiki/Lanczos_approximation
 http://en.wikipedia.org/wiki/Stirling%27s_approximation
 http://www.gnu.org/software/gsl/
 http://jakarta.apache.org/commons/math/
 http://my.fit.edu/~gabdo/gamma.txt
 */

/**
 * @author Mihai Preda, original author
 * @author Ke Zhai, add in trigamma and digamma function approximations.
 */
public class Gamma {
  private static String ulps(double v, double ref) {
    double ulp = ref == 0 ? Math.ulp(.1) : Math.ulp(ref);
    int ulps = (int) Math.floor((v - ref) / ulp + .5);
    // return ulps != 0 ? ""+ulps : "";
    return "" + ulps;
  }

  public static void main(String argv[]) {
    double a, b, c, d, e;
    for (int i = 1; i < 171; ++i) {
      a = Math.log(factorial(i));
      b = lgamma(i + 1);
      e = lanczosLGamma15(i);
      c = f(i);
      d = stirlingLGamma(i);

      System.out.printf("%3d | %6s | %6s | %6s | %6s |\n", i, ulps(b, a), ulps(e, a), ulps(c, a),
          i >= 10 ? ulps(d, a) : "-1000+");
    }

    boolean doBenchmark = true;
    if (doBenchmark) {
      final int N = 20000;
      long t1, t2;

      t1 = System.currentTimeMillis();
      for (int r = 0; r < N; ++r) {
        for (int i = 1; i < 171; ++i) {
          b = lgamma(i + 1);
        }
      }
      t2 = System.currentTimeMillis();
      System.out.println("fdlibm's  : " + (t2 - t1));

      t1 = System.currentTimeMillis();
      for (int r = 0; r < N; ++r) {
        for (int i = 1; i < 171; ++i) {
          e = lanczosLGamma15(i);
        }
      }
      t2 = System.currentTimeMillis();
      System.out.println("Lanczos 15: " + (t2 - t1));

      t1 = System.currentTimeMillis();
      for (int r = 0; r < N; ++r) {
        for (int i = 1; i < 171; ++i) {
          c = f(i);
        }
      }
      t2 = System.currentTimeMillis();
      System.out.println("f : " + (t2 - t1));

      t1 = System.currentTimeMillis();
      for (int r = 0; r < N; ++r) {
        for (int i = 1; i < 171; ++i) {
          d = stirlingLGamma(i);
        }
      }
      t2 = System.currentTimeMillis();
      System.out.println("Stirling  :  " + (t2 - t1));
    }
  }

  private static final double zero = 0.0;
  private static final double one = 1.0;
  private static final double half = 0.5;
  private static final double SQRT2PI = 2.50662827463100024157;
  private static final double LN_SQRT2PI = 0.9189385332046727418;

  private static final int HI(double x) {
    return (int) (Double.doubleToLongBits(x) >> 32);
  }

  private static final int LO(double x) {
    return (int) Double.doubleToLongBits(x);
  }

  // coefficients for gamma=7, kmax=8 Lanczos method
  private static final double L9[] = { 0.99999999999980993227684700473478,
      676.520368121885098567009190444019, -1259.13921672240287047156078755283,
      771.3234287776530788486528258894, -176.61502916214059906584551354,
      12.507343278686904814458936853, -0.13857109526572011689554707, 9.984369578019570859563e-6,
      1.50563273514931155834e-7 };
  private static final double SQRT2PI_E7 = 0.0022857491179850424; // sqrt(2*pi)/e**7

  static final double lanczosGamma9(double x) {
    if (x <= -1)
      return Double.NaN;
    double a = L9[0];
    for (int i = 1; i < 9; ++i) {
      a += L9[i] / (x + i);
    }
    return (SQRT2PI_E7 * a) * Math.pow((x + 7.5) / Math.E, x + .5);
  }

  static final double lanczosLGamma9(double x) {
    if (x <= -1)
      return Double.NaN;
    double a = L9[0];
    for (int i = 1; i < 9; ++i) {
      a += L9[i] / (x + i);
    }
    return (LN_SQRT2PI + Math.log(a) - 7.) + (x + .5) * Math.log((x + 7.5) / Math.E);
  }

  private static final double[] L15 = { 0.99999999999999709182, 57.156235665862923517,
      -59.597960355475491248, 14.136097974741747174, -0.49191381609762019978,
      .33994649984811888699e-4, .46523628927048575665e-4, -.98374475304879564677e-4,
      .15808870322491248884e-3, -.21026444172410488319e-3, .21743961811521264320e-3,
      -.16431810653676389022e-3, .84418223983852743293e-4, -.26190838401581408670e-4,
      .36899182659531622704e-5, };
  private static final double G_PLUS_HALF = 607 / 128. + .5;

  static final double lanczosLGamma15(double x) {
    if (x <= -1)
      return Double.NaN;
    double a = L15[0];
    for (int i = 1; i < 15; ++i) {
      a += L15[i] / (x + i);
    }

    double tmp = x + G_PLUS_HALF;
    return (LN_SQRT2PI + Math.log(a)) + (x + .5) * Math.log(tmp) - tmp;
  }

  static final double g(double x) {
    if (x <= -1)
      return Double.NaN;
    double tmp = x + 5.2421875;
    return 0.9189385332046727418
        + Math.log(0.99999999999999709182 + 57.156235665862923517 / (x + 1)
            + -59.597960355475491248 / (x + 2) + 14.136097974741747174 / (x + 3)
            + -0.49191381609762019978 / (x + 4) + .33994649984811888699e-4 / (x + 5)
            + .46523628927048575665e-4 / (x + 6) + -.98374475304879564677e-4 / (x + 7)
            + .15808870322491248884e-3 / (x + 8) + -.21026444172410488319e-3 / (x + 9)
            + .21743961811521264320e-3 / (x + 10) + -.16431810653676389022e-3 / (x + 11)
            + .84418223983852743293e-4 / (x + 12) + -.26190838401581408670e-4 / (x + 13)
            + .36899182659531622704e-5 / (x + 14)) + (x + .5) * Math.log(tmp) - tmp;
  }

  static final double f(double x) {
    if (x <= -1)
      return Double.NaN;
    final double tmp = x + 5.2421875;
    // final double saveX = x;
    return 0.9189385332046727418
        + Math.log(0.99999999999999709182 + 57.156235665862923517 / ++x + -59.597960355475491248
            / ++x + 14.136097974741747174 / ++x + -0.49191381609762019978 / ++x
            + .33994649984811888699e-4 / ++x + .46523628927048575665e-4 / ++x
            + -.98374475304879564677e-4 / ++x + .15808870322491248884e-3 / ++x
            + -.21026444172410488319e-3 / ++x + .21743961811521264320e-3 / ++x
            + -.16431810653676389022e-3 / ++x + .84418223983852743293e-4 / ++x
            + -.26190838401581408670e-4 / ++x + .36899182659531622704e-5 / ++x) + (tmp - 4.7421875)
        * Math.log(tmp) - tmp
    // + (saveX + .5)*Math.log(tmp) + /*Math.sqrt(tmp)*/ - tmp
    ;
  }

  private static final double SC1 = 0.08333333333333333, SC2 = 0.003472222222222222,
      SC3 = -0.0026813271604938273, SC4 = -2.2947209362139917E-4, LC1 = 0.08333333333333333,
      LC2 = -0.002777777777777778, LC3 = 7.936507936507937E-4, LC4 = -5.952380952380953E-4;

  static final double stirlingGamma(double x) {
    final double r1 = 1. / x, r2 = r1 * r1, r4 = r2 * r2;
    return SQRT2PI * Math.sqrt(x) * (1 + SC1 * r1 + SC2 * r2 + SC3 * r1 * r2 + SC4 * r4)
        * Math.pow(x / Math.E, x);
  }

  static final double stirlingLGamma(double x) {
    final double r1 = 1. / x, r2 = r1 * r1, r3 = r1 * r2, r5 = r2 * r3, r7 = r3 * r3 * r1;
    return (x + .5) * Math.log(x) - x + LN_SQRT2PI + LC1 * r1 + LC2 * r3 + LC3 * r5 + LC4 * r7;
  }

  static final double FACT[] = { 1.0, 40320.0, 2.0922789888E13, 6.204484017332394E23,
      2.631308369336935E35, 8.159152832478977E47, 1.2413915592536073E61, 7.109985878048635E74,
      1.2688693218588417E89, 6.1234458376886085E103, 7.156945704626381E118, 1.8548264225739844E134,
      9.916779348709496E149, 1.0299016745145628E166, 1.974506857221074E182, 6.689502913449127E198,
      3.856204823625804E215, 3.659042881952549E232, 5.5502938327393044E249, 1.3113358856834524E267,
      4.7147236359920616E284, 2.5260757449731984E302, };

  static final double factorial(double x) {
    if (x <= -1) {
      return Double.NaN;
    }
    if (x <= 170) {
      if (Math.floor(x) == x) {
        int n = (int) x;
        double extra = x;
        switch (n & 7) {
        case 7:
          extra *= --x;
        case 6:
          extra *= --x;
        case 5:
          extra *= --x;
        case 4:
          extra *= --x;
        case 3:
          extra *= --x;
        case 2:
          extra *= --x;
        case 1:
          return FACT[n >> 3] * extra;
        case 0:
          return FACT[n >> 3];
        }
      }
    }
    return Math.exp(lgamma(x + 1));
  }

  private static final double a0 = 7.72156649015328655494e-02, a1 = 3.22467033424113591611e-01,
      a2 = 6.73523010531292681824e-02, a3 = 2.05808084325167332806e-02,
      a4 = 7.38555086081402883957e-03, a5 = 2.89051383673415629091e-03,
      a6 = 1.19270763183362067845e-03, a7 = 5.10069792153511336608e-04,
      a8 = 2.20862790713908385557e-04, a9 = 1.08011567247583939954e-04,
      a10 = 2.52144565451257326939e-05, a11 = 4.48640949618915160150e-05,
      tc = 1.46163214496836224576e+00, tf = -1.21486290535849611461e-01,
      tt = -3.63867699703950536541e-18, t0 = 4.83836122723810047042e-01,
      t1 = -1.47587722994593911752e-01, t2 = 6.46249402391333854778e-02,
      t3 = -3.27885410759859649565e-02, t4 = 1.79706750811820387126e-02,
      t5 = -1.03142241298341437450e-02, t6 = 6.10053870246291332635e-03,
      t7 = -3.68452016781138256760e-03, t8 = 2.25964780900612472250e-03,
      t9 = -1.40346469989232843813e-03, t10 = 8.81081882437654011382e-04,
      t11 = -5.38595305356740546715e-04, t12 = 3.15632070903625950361e-04,
      t13 = -3.12754168375120860518e-04, t14 = 3.35529192635519073543e-04,
      u0 = -7.72156649015328655494e-02, u1 = 6.32827064025093366517e-01,
      u2 = 1.45492250137234768737e+00, u3 = 9.77717527963372745603e-01,
      u4 = 2.28963728064692451092e-01, u5 = 1.33810918536787660377e-02,
      v1 = 2.45597793713041134822e+00, v2 = 2.12848976379893395361e+00,
      v3 = 7.69285150456672783825e-01, v4 = 1.04222645593369134254e-01,
      v5 = 3.21709242282423911810e-03, s0 = -7.72156649015328655494e-02,
      s1 = 2.14982415960608852501e-01, s2 = 3.25778796408930981787e-01,
      s3 = 1.46350472652464452805e-01, s4 = 2.66422703033638609560e-02,
      s5 = 1.84028451407337715652e-03, s6 = 3.19475326584100867617e-05,
      r1 = 1.39200533467621045958e+00, r2 = 7.21935547567138069525e-01,
      r3 = 1.71933865632803078993e-01, r4 = 1.86459191715652901344e-02,
      r5 = 7.77942496381893596434e-04, r6 = 7.32668430744625636189e-06,
      w0 = 4.18938533204672725052e-01, w1 = 8.33333333333329678849e-02,
      w2 = -2.77777777728775536470e-03, w3 = 7.93650558643019558500e-04,
      w4 = -5.95187557450339963135e-04, w5 = 8.36339918996282139126e-04,
      w6 = -1.63092934096575273989e-03;

  static final double lgamma(double x) {
    double t, y, z, p, p1, p2, p3, q, r, w;
    int i;

    int hx = HI(x);
    int lx = LO(x);

    /* purge off +-inf, NaN, +-0, and negative arguments */
    int ix = hx & 0x7fffffff;
    if (ix >= 0x7ff00000)
      return Double.POSITIVE_INFINITY;
    if ((ix | lx) == 0 || hx < 0)
      return Double.NaN;
    if (ix < 0x3b900000) { /* |x|<2**-70, return -log(|x|) */
      return -Math.log(x);
    }

    /* purge off 1 and 2 */
    if ((((ix - 0x3ff00000) | lx) == 0) || (((ix - 0x40000000) | lx) == 0))
      r = 0;
    /* for x < 2.0 */
    else if (ix < 0x40000000) {
      if (ix <= 0x3feccccc) { /* lgamma(x) = lgamma(x+1)-log(x) */
        r = -Math.log(x);
        if (ix >= 0x3FE76944) {
          y = one - x;
          i = 0;
        } else if (ix >= 0x3FCDA661) {
          y = x - (tc - one);
          i = 1;
        } else {
          y = x;
          i = 2;
        }
      } else {
        r = zero;
        if (ix >= 0x3FFBB4C3) {
          y = 2.0 - x;
          i = 0;
        } /* [1.7316,2] */
        else if (ix >= 0x3FF3B4C4) {
          y = x - tc;
          i = 1;
        } /* [1.23,1.73] */
        else {
          y = x - one;
          i = 2;
        }
      }

      switch (i) {
      case 0:
        z = y * y;
        p1 = a0 + z * (a2 + z * (a4 + z * (a6 + z * (a8 + z * a10))));
        p2 = z * (a1 + z * (a3 + z * (a5 + z * (a7 + z * (a9 + z * a11)))));
        p = y * p1 + p2;
        r += (p - 0.5 * y);
        break;
      case 1:
        z = y * y;
        w = z * y;
        p1 = t0 + w * (t3 + w * (t6 + w * (t9 + w * t12))); /*
                                                             * parallel comp
                                                             */
        p2 = t1 + w * (t4 + w * (t7 + w * (t10 + w * t13)));
        p3 = t2 + w * (t5 + w * (t8 + w * (t11 + w * t14)));
        p = z * p1 - (tt - w * (p2 + y * p3));
        r += (tf + p);
        break;
      case 2:
        p1 = y * (u0 + y * (u1 + y * (u2 + y * (u3 + y * (u4 + y * u5)))));
        p2 = one + y * (v1 + y * (v2 + y * (v3 + y * (v4 + y * v5))));
        r += (-0.5 * y + p1 / p2);
      }
    } else if (ix < 0x40200000) { /* x < 8.0 */
      i = (int) x;
      t = zero;
      y = x - (double) i;
      p = y * (s0 + y * (s1 + y * (s2 + y * (s3 + y * (s4 + y * (s5 + y * s6))))));
      q = one + y * (r1 + y * (r2 + y * (r3 + y * (r4 + y * (r5 + y * r6)))));
      r = half * y + p / q;
      z = one; /* lgamma(1+s) = log(s) + lgamma(s) */
      switch (i) {
      case 7:
        z *= (y + 6.0); /* FALLTHRU */
      case 6:
        z *= (y + 5.0); /* FALLTHRU */
      case 5:
        z *= (y + 4.0); /* FALLTHRU */
      case 4:
        z *= (y + 3.0); /* FALLTHRU */
      case 3:
        z *= (y + 2.0); /* FALLTHRU */
        r += Math.log(z);
        break;
      }
      /* 8.0 <= x < 2**58 */
    } else if (ix < 0x43900000) {
      t = Math.log(x);
      z = one / x;
      y = z * z;
      w = w0 + z * (w1 + y * (w2 + y * (w3 + y * (w4 + y * (w5 + y * w6)))));
      r = (x - half) * (t - one) + w;
    } else
      /* 2**58 <= x <= inf */
      r = x * (Math.log(x) - one);
    return r;
  }

  /**
   * Approximate digamma of x.
   */
  public static double digamma(double x) {
    double r = 0.0;

    while (x <= 5) {
      r -= 1 / x;
      x += 1;
    }

    double f = 1.0 / (x * x);
    // double t = f * (-1.0 / 12.0 + f * (1.0 / 120.0 + f * (-1.0 / 252.0 + f * (1.0 / 240.0 + f
    // * (-1.0 / 132.0 + f * (691.0 / 32760.0 + f * (-1.0 / 12.0 + f * 3617.0 / 8160.0)))))));
    double t = f
        * (-0.0833333333333333333333333333333 + f
            * (0.00833333333333333333333333333333 + f
                * (-0.00396825396825396825 + f
                    * (0.0041666666666666666666666667 + f
                        * (-0.00757575757575757575757575757576 + f
                            * (0.0210927960928 + f
                                * (-0.0833333333333333333333333333333 + f * 0.44325980392157)))))));
    return r + Math.log(x) - 0.5 / x + t;
  }

  /**
   * Approximate the trigamma of x.
   */
  public static double trigamma(double x) {
    double p;
    int i;

    x = x + 6;
    p = 1 / (x * x);
    p = (((((0.075757575757576 * p - 0.033333333333333) * p + 0.0238095238095238) * p - 0.033333333333333)
        * p + 0.166666666666667)
        * p + 1)
        / x + 0.5 * p;
    for (i = 0; i < 6; i++) {
      x = x - 1;
      p = 1 / (x * x) + p;
    }

    Preconditions.checkArgument(!Double.isNaN(p), new ArithmeticException(
        "invalid input at trigamma function: " + x));

    if (Double.isNaN(p)) {
      throw new ArithmeticException("invalid input at trigamma function: " + x);
    }

    return p;
  }

  public static double lngamma(double x) {
    return lgamma(x);
  }

  /**
   * @deprecated
   * @param x
   */
  public static double logGamma(double x) {
    double z = 1 / (x * x);

    x = x + 6;
    z = (((-0.000595238095238 * z + 0.000793650793651) * z - 0.002777777777778) * z + 0.083333333333333)
        / x;
    z = (x - 0.5) * Math.log(x) - x + 0.918938533204673 + z - Math.log(x - 1) - Math.log(x - 2)
        - Math.log(x - 3) - Math.log(x - 4) - Math.log(x - 5) - Math.log(x - 6);

    if (new Double(z).equals(Double.NaN)) {
      throw new ArithmeticException("invalid input at lnGamma function: " + x);
    }

    return z;
  }
}