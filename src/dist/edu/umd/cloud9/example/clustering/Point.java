package edu.umd.cloud9.example.clustering;

public class Point {
  public double value;

  public Point(double value) {
    this.value = value;
  }

  public Point clone() {
    return new Point(value);
  }
}
