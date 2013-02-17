package edu.umd.cloud9.example.clustering;

import java.util.Arrays;
import java.util.Random;
import java.util.Vector;

public class KMeans {

	private static final int MAX_ITERATIONS = 30;
	
	
	/**
	 * Performs a k-means on the point set to compute k clusters.
	 * @param  points  point set
	 * @param  k       number of clusters
	 * @return         clusters
	 */
	public static Vector<PVector>[] run(PVector[] points, int k){
		
		PVector[]         centroids   = initialize(points, k);
		int[]             repartition = new int[points.length];
		Vector<PVector>[] clusters    = new Vector[k];
		
		int   it  = 0;      
		int[] tmp = new int[points.length];
		
		do{
			tmp = repartition.clone();
			repartitionStep(points, k, centroids, repartition, clusters);
			centroidStep(points, k, centroids, clusters);
			it++;
		} while(!Arrays.equals(repartition, tmp) && it<MAX_ITERATIONS);
		
		return clusters;
	}
	
	
	/**
	 * Initializes the k-means by ramdomly picking points in the set.
	 * @param  points  point set
	 * @param  k       number of clusters
	 * @return         clusters
	 */
	private static PVector[] initialize(PVector[] points, int k){
		
		// Initialize the first centroid
		PVector[] centroids = new PVector[k];
		Random    rand      = new Random();
		centroids[0]        = (PVector)points[rand.nextInt(points.length)].clone();
		
		// Initialize the other centroids
		for (int i=1; i<k; i++){
			boolean cond = false;
			PVector tmp;
			do{
				cond = false;
				tmp  = points[rand.nextInt(points.length)];
				for (int j=0; j<i; j++){
					if (PVector.equals(tmp, centroids[j])){
						cond = true;
						break;
					}
				}
			}while(cond);
			centroids[i] = (PVector)tmp.clone();
		}
		
		// Return
		return centroids;
	}
	
	
	/**
	 * Processes the repartition step.
	 * @param points       point set
	 * @param k            number of clusters
	 * @param centroids    centroids of the clusters
	 * @param repartition  repartition array
	 * @param clusters     clusters
	 */
	private static void repartitionStep(PVector[] points, int k, PVector[] centroids, int[] repartition, Vector<PVector>[] clusters){
		
		// Initialization of the clusters
		for (int i=0; i<k; i++)
			clusters[i] = new Vector<PVector>();
		
		// Compute repartition
		for (int i=0; i<points.length; i++){
		
			int    index = 0;
			double dist  = Double.MAX_VALUE;
			
			for (int j=0; j<k; j++){
				double dist_tmp = points[i].Minus(centroids[j]).norm2();
				if (dist_tmp<dist){
					dist  = dist_tmp;
					index = j;
				}
			}

			repartition[i] = index;
			clusters[index].add(points[i]);	
		}
	}
	
	
	/**
	 * Processes the centroid step.
	 * @param points     point set
	 * @param k          number of clusters
	 * @param centroids  centroids of the clusters
	 * @param clusters   clusters
	 */
	private static void centroidStep(PVector[] points, int k, PVector[] centroids, Vector<PVector>[] clusters){
		for (int i=0; i<k; i++){
			centroids[i] = new PVector(points[0].dim);
			for (int j=0; j<clusters[i].size(); j++)
				centroids[i] = centroids[i].Plus((PVector)clusters[i].get(j));
			centroids[i] = centroids[i].Times(1.0d/clusters[i].size());
		}
	}
}
