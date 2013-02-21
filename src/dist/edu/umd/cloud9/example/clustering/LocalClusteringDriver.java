package edu.umd.cloud9.example.clustering;

import java.util.Vector;

public class LocalClusteringDriver {

  public static void main(String[] args) {
    // Variables
    int n = 3;
    int m = 10000;

    // Initial mixture model
    MixtureModel mm = new MixtureModel(n);
    mm.EF = new UnivariateGaussian();
    for (int i = 0; i < n; i++) {
      PVector param = new PVector(2);
      param.array[0] = 10 * (i + 1);
      param.array[1] = 2 * (i + 1);
      mm.param[i] = param;
      mm.weight[i] = i + 1;
    }
    mm.normalizeWeights();
    System.out.println("Initial mixure model \n" + mm + "\n");

    // Draw points from initial mixture model and compute the n clusters
    PVector[] points = mm.drawRandomPoints(m);
    Vector<PVector>[] clusters = KMeans.run(points, n);

    // Classical EM
    MixtureModel mmc;
    mmc = ExpectationMaximization1D.initialize(clusters);
    mmc = ExpectationMaximization1D.run(points, mmc);
    System.out.println("Mixure model estimated using classical EM \n" + mmc + "\n");
  }
}