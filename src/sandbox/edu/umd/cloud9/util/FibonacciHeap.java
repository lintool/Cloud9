/*
 * The contents of this file are subject to the terms of the Common Development
 * and Distribution License (the License). You may not use this file except in
 * compliance with the License.
 *
 * You can obtain a copy of the License at http://www.netbeans.org/cddl.html
 * or http://www.netbeans.org/cddl.txt.
 *
 * When distributing Covered Code, include this CDDL Header Notice in each file
 * and include the License file at http://www.netbeans.org/cddl.txt.
 * If applicable, add the following below the CDDL Header, with the fields
 * enclosed by brackets [] replaced by your own identifying information:
 * "Portions Copyrighted [year] [name of copyright owner]"
 *
 * The Original Software is GraphMaker. The Initial Developer of the Original
 * Software is Nathan L. Fiedler. Portions created by Nathan L. Fiedler
 * are Copyright (C) 1999-2008. All Rights Reserved.
 *
 * Contributor(s): Nathan L. Fiedler.
 *
 * $Id: FibonacciHeap.java 20 2008-12-05 15:51:14Z nathanfiedler $
 */
package edu.umd.cloud9.util;

/**
 * This class implements a Fibonacci heap data structure. Much of the
 * code in this class is based on the algorithms in Chapter 21 of the
 * "Introduction to Algorithms" by Cormen, Leiserson, Rivest, and Stein.
 * The amortized running time of most of these methods is O(1), making
 * it a very fast data structure. Several have an actual running time
 * of O(1). removeMin() and delete() have O(log n) amortized running
 * times because they do the heap consolidation.
 *
 * <p><strong>Note that this implementation is not synchronized.</strong>
 * If multiple threads access a set concurrently, and at least one of the
 * threads modifies the set, it <em>must</em> be synchronized externally.
 * This is typically accomplished by synchronizing on some object that
 * naturally encapsulates the set.</p>
 *
 * @author  Nathan Fiedler
 */
public class FibonacciHeap<T extends Comparable> {
    /** Points to the minimum node in the heap. */
    private Node<T> min;
    /** Number of nodes in the heap. If the type is ever widened,
     * (e.g. changed to long) then recalcuate the maximum degree
     * value used in the consolidate() method. */
    private int n;

    /**
     * Removes all elements from this heap.
     *
     * <p><em>Running time: O(1)</em></p>
     */
    public void clear() {
        min = null;
        n = 0;
    }

    /**
     * Consolidates the trees in the heap by joining trees of equal
     * degree until there are no more trees of equal degree in the
     * root list.
     *
     * <p><em>Running time: O(log n) amortized</em></p>
     */
    private void consolidate() {
        // The magic 45 comes from log base phi of Integer.MAX_VALUE,
        // which is the most elements we will ever hold, and log base
        // phi represents the largest degree of any root list node.
    	@SuppressWarnings("unchecked")
        Node<T>[] A = new Node[45];

        // For each root list node look for others of the same degree.
        Node<T> start = min;
        Node<T> w = min;
        do {
            Node<T> x = w;
            // Because x might be moved, save its sibling now.
            Node<T> nextW = w.right;
            int d = x.degree;
            while (A[d] != null) {
                // Make one of the nodes a child of the other.
                Node<T> y = A[d];
                if (x.isGreaterThan(y)) {
                    Node<T> temp = y;
                    y = x;
                    x = temp;
                }
                if (y == start) {
                    // Because removeMin() arbitrarily assigned the min
                    // reference, we have to ensure we do not miss the
                    // end of the root node list.
                    start = start.right;
                }
                if (y == nextW) {
                    // If we wrapped around we need to check for this case.
                    nextW = nextW.right;
                }
                // Node y disappears from root list.
                y.link(x);
                // We've handled this degree, go to next one.
                A[d] = null;
                d++;
            }
            // Save this node for later when we might encounter another
            // of the same degree.
            A[d] = x;
            // Move forward through list.
            w = nextW;
        } while (w != start);

        // The node considered to be min may have been changed above.
        min = start;
        // Find the minimum key again.
        for (Node<T> a : A) {
            if (a != null && a.isLessThan(min) ) {
                min = a;
            }
        }
    }

    /**
     * Decreases the key value for a heap node, given the new value
     * to take on. The structure of the heap may be changed, but will
     * not be consolidated.
     *
     * <p><em>Running time: O(1) amortized</em></p>
     *
     * @param  x  node to decrease the key of
     * @param  k  new key value for node x
     * @exception  IllegalArgumentException
     *             if k is larger than x.key value.
     */
    public void decreaseKey(Node<T> x, float k) {
        decreaseKey(x, k, false);
    }

    /**
     * Decrease the key value of a node, or simply bubble it up to the
     * top of the heap in preparation for a delete operation.
     *
     * @param  x       node to decrease the key of.
     * @param  k       new key value for node x.
     * @param  delete  true if deleting node (in which case, k is ignored).
     */
    private void decreaseKey(Node<T> x, float k, boolean delete) {
        if (!delete && k > x.key) {
            throw new IllegalArgumentException("cannot increase key value");
        }
        x.key = k;
        Node<T> y = x.parent;
        if (y != null && (delete || x.isLessThan(y))) {
            y.cut(x, min);
            y.cascadingCut(min);
        }
        if (delete || x.isLessThan(min)) {
            min = x;
        }
    }

    /**
     * Deletes a node from the heap given the reference to the node.
     * The trees in the heap will be consolidated, if necessary.
     *
     * <p><em>Running time: O(log n) amortized</em></p>
     *
     * @param  x  node to remove from heap.
     */
    public void delete(Node<T> x) {
        // make x as small as possible
        decreaseKey(x, 0, true);
        // remove the smallest, which decreases n also
        removeMin();
    }

    /**
     * Tests if the Fibonacci heap is empty or not. Returns true if
     * the heap is empty, false otherwise.
     *
     * <p><em>Running time: O(1)</em></p>
     *
     * @return  true if the heap is empty, false otherwise.
     */
    public boolean isEmpty() {
        return min == null;
    }

    /**
     * Inserts a new data element into the heap. No heap consolidation
     * is performed at this time, the new node is simply inserted into
     * the root list of this heap.
     *
     * <p><em>Running time: O(1)</em></p>
     *
     * @param  x    data object to insert into heap.
     * @param  key  key value associated with data object.
     * @return newly created heap node.
     */
    public Node<T> insert(T x, float key) {
        Node<T> node = new Node<T>(x, key);
        // concatenate node into min list
        if (min != null) {
            node.right = min;
            node.left = min.left;
            min.left = node;
            node.left.right = node;
            if (node.isLessThan(min)) {
                min = node;
            }
        } else {
            min = node;
        }
        n++;
        return node;
    }

    /**
     * Returns the smallest element in the heap. This smallest element
     * is the one with the minimum key value.
     *
     * <p><em>Running time: O(1)</em></p>
     *
     * @return  heap node with the smallest key, or null if empty.
     */
    public Node<T> min() {
        return min;
    }

    /**
     * Removes the smallest element from the heap. This will cause
     * the trees in the heap to be consolidated, if necessary.
     *
     * <p><em>Running time: O(log n) amortized</em></p>
     *
     * @return  data object with the smallest key.
     */
    public Node<T> removeMin() {
        Node<T> z = min;
        if (z == null) {
            return null;
        }
        if (z.child != null) {
            z.child.parent = null;
            // for each child of z do...
            for (Node<T> x = z.child.right; x != z.child; x = x.right) {
                // set parent[x] to null
                x.parent = null;
            }
            // merge the children into root list
            Node<T> minleft = min.left;
            Node<T> zchildleft = z.child.left;
            min.left = zchildleft;
            zchildleft.right = min;
            z.child.left = minleft;
            minleft.right = z.child;
        }
        // remove z from root list of heap
        z.left.right = z.right;
        z.right.left = z.left;
        if (z == z.right) {
            min = null;
        } else {
            min = z.right;
            consolidate();
        }
        // decrement size of heap
        n--;
        return z;
    }

    /**
     * Returns the size of the heap which is measured in the
     * number of elements contained in the heap.
     *
     * <p><em>Running time: O(1)</em></p>
     *
     * @return  number of elements in the heap.
     */
    public int size() {
        return n;
    }

    /**
     * Joins two Fibonacci heaps into a new one. No heap consolidation is
     * performed at this time. The two root lists are simply joined together.
     *
     * <p><em>Running time: O(1)</em></p>
     *
     * @param  H1  first heap
     * @param  H2  second heap
     * @return  new heap containing H1 and H2
     */
    public static <T extends Comparable<T>> FibonacciHeap<T> union(FibonacciHeap<T> H1, FibonacciHeap<T> H2) {
        FibonacciHeap<T> H = new FibonacciHeap<T>();
        if (H1 != null && H2 != null) {
            H.min = H1.min;
            if (H.min != null) {
                if (H2.min != null) {
                    H.min.right.left = H2.min.left;
                    H2.min.left.right = H.min.right;
                    H.min.right = H2.min;
                    H2.min.left = H.min;
                    if (H2.min.isLessThan(H1.min)) {
                        H.min = H2.min;
                    }
                }
            } else {
                H.min = H2.min;
            }
            H.n = H1.n + H2.n;
        }
        return H;
    }

    /**
     * Implements a node of the Fibonacci heap. It holds the information
     * necessary for maintaining the structure of the heap. It acts as
     * an opaque handle for the data element, and serves as the key to
     * retrieving the data from the heap.
     *
     * @author  Nathan Fiedler
     */
    public static class Node<T extends Comparable> {
        /** Data object for this node, holds the key value. */
        private T datum;
        /** Key value for this node. */
        private float key;
        /** Parent node. */
        private Node<T> parent;
        /** First child node. */
        private Node<T> child;
        /** Right sibling node. */
        private Node<T> right;
        /** Left sibling node. */
        private Node<T> left;
        /** Number of children of this node. */
        private int degree;
        /** True if this node has had a child removed since this node was
         * added to its parent. */
        private boolean mark;

        /**
         * Two-arg constructor which sets the data and key fields to the
         * passed arguments. It also initializes the right and left pointers,
         * making this a circular doubly-linked list.
         *
         * @param  data  data object to associate with this node
         * @param  key   key value for this data object
         */
        public Node(T data, float key) {
            this.datum = data;
            this.key = key;
            right = this;
            left = this;
        }

        /**
         * Performs a cascading cut operation. Cuts this from its parent
         * and then does the same for its parent, and so on up the tree.
         *
         * <p><em>Running time: O(log n)</em></p>
         *
         * @param  min  the minimum heap node, to which nodes will be added.
         */
        public void cascadingCut(Node<T> min) {
            Node<T> z = parent;
            // if there's a parent...
            if (z != null) {
                if (mark) {
                    // it's marked, cut it from parent
                    z.cut(this, min);
                    // cut its parent as well
                    z.cascadingCut(min);
                } else {
                    // if y is unmarked, set it marked
                    mark = true;
                }
            }
        }

        /**
         * The reverse of the link operation: removes x from the child
         * list of this node.
         *
         * <p><em>Running time: O(1)</em></p>
         *
         * @param  x    child to be removed from this node's child list
         * @param  min  the minimum heap node, to which x is added.
         */
        public void cut(Node<T> x, Node<T> min) {
            // remove x from childlist and decrement degree
            x.left.right = x.right;
            x.right.left = x.left;
            degree--;
            // reset child if necessary
            if (degree == 0) {
                child = null;
            } else if (child == x) {
                child = x.right;
            }
            // add x to root list of heap
            x.right = min;
            x.left = min.left;
            min.left = x;
            x.left.right = x;
            // set parent[x] to nil
            x.parent = null;
            // set mark[x] to false
            x.mark = false;
        }

        /**
         * Make this node a child of the given parent node. All linkages
         * are updated, the degree of the parent is incremented, and
         * mark is set to false.
         *
         * @param  parent  the new parent node.
         */
        public void link(Node<T> parent) {
            // Note: putting this code here in Node makes it 7x faster
            // because it doesn't have to use generated accessor methods,
            // which add a lot of time when called millions of times.
            // remove this from its circular list
            left.right = right;
            right.left = left;
            // make this a child of x
            this.parent = parent;
            if (parent.child == null) {
                parent.child = this;
                right = this;
                left = this;
            } else {
                left = parent.child;
                right = parent.child.right;
                parent.child.right = this;
                right.left = this;
            }
            // increase degree[x]
            parent.degree++;
            // set mark false
            mark = false;
        }
        
        public T getDatum() {
        	return this.datum;
        }

        public float getKey() {
        	return this.key;
        }

        private boolean isGreaterThan(Node n) {
        	if ( this.key > n.key || ( this.key == n.key && this.datum.compareTo((T) n.datum) > 0))
        		return true;
        	
        	return false;
        }

        private boolean isLessThan(Node n) {
        	if ( this.key < n.key || ( this.key == n.key && this.datum.compareTo((T) n.datum) < 0))
        		return true;
        	
        	return false;
        }
    }
}
