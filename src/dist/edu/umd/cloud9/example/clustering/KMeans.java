package edu.umd.cloud9.example.clustering;

import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

public class KMeans {

  private static final int MAX_ITERATIONS = 30;

  /**
   * Performs a k-means on the point set to compute k clusters.
   *
   * @param points point set
   * @param k number of clusters
   * @return clusters
   */
  public static List<Point>[] run(Point[] points, int k) {
    Point[] centroids = initialize(points, k);
    int[] repartition = new int[points.length];
    @SuppressWarnings("unchecked")
    List<Point>[] clusters = (List<Point>[]) new List[k];

    int it = 0;
    int[] tmp = new int[points.length];

    do {
      tmp = repartition.clone();
      repartitionStep(points, k, centroids, repartition, clusters);
      centroidStep(points, k, centroids, clusters);
      it++;
    } while (!Arrays.equals(repartition, tmp) && it < MAX_ITERATIONS);

    return clusters;
  }

  public static void dumpClusters(List<Point>[] clusters){
    for (List<Point> cluster : clusters) {
      System.out.println(Lists.transform(cluster, new Function<Point, Double>() {

        @Override @Nullable
        public Double apply(@Nullable Point point) {
          return point.value;
        }
        
      }).toString());
    }
  }
  
  /**
   * Initializes the k-means by randomly picking points in the set.
   *
   * @param points point set
   * @param k number of clusters
   * @return clusters
   */
  private static Point[] initialize(Point[] points, int k) {
    Integer[] arr = ExpectationMaximization.sampleNUniquePoints(k, points.length);
    Point[] centroids = new Point[k];
    for (int i=0; i<k; i++) {
      centroids[i] = new Point(points[arr[i]].value);
    }
    // Return
    return centroids;
  }

  /**
   * Processes the repartition step.
   *
   * @param points point set
   * @param k number of clusters
   * @param centroids centroids of the clusters
   * @param repartition repartition array
   * @param clusters clusters
   */
  private static void repartitionStep(Point[] points, int k, Point[] centroids,
      int[] repartition, List<Point>[] clusters) {

    // Initialization of the clusters
    for (int i = 0; i < k; i++)
      clusters[i] = Lists.newArrayList();

    // Compute repartition
    for (int i = 0; i < points.length; i++) {

      int index = 0;
      double dist = Double.MAX_VALUE;

      for (int j = 0; j < k; j++) {
        double dist_tmp = Math.abs(points[i].value - centroids[j].value);
        if (dist_tmp < dist) {
          dist = dist_tmp;
          index = j;
        }
      }

      repartition[i] = index;
      clusters[index].add(points[i]);
    }
  }

  /**
   * Processes the centroid step.
   *
   * @param points point set
   * @param k number of clusters
   * @param centroids centroids of the clusters
   * @param clusters clusters
   */
  private static void centroidStep(Point[] points, int k, Point[] centroids,
      List<Point>[] clusters) {
    for (int i = 0; i < k; i++) {
      centroids[i] = new Point(0);
      for (int j = 0; j < clusters[i].size(); j++) {
        centroids[i].value = centroids[i].value + clusters[i].get(j).value;
      }
      centroids[i].value = centroids[i].value * (1.0d / clusters[i].size());
    }
  }
}
