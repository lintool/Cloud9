package edu.umd.cloud9.math;

public class LogMath {
  /**
   * 
   * @param a log a, in natural base e
   * @param b log b, in natural base e
   * @return log(a + b), in natural base e
   */
  public static double add(double a, double b) {
    if (a < b) {
      return b + Math.log(1 + Math.exp(a - b));
    } else {
      return a + Math.log(1 + Math.exp(b - a));
    }
  }

  /**
   * 
   * @param a log a, in natural base e
   * @param b log b, in natural base e
   * @return log(a + b), in natural base e
   */
  public static float add(float a, float b) {
    if (a < b) {
      return (float) (b + Math.log(1 + Math.exp(a - b)));
    } else {
      return (float) (a + Math.log(1 + Math.exp(b - a)));
    }
  }
}