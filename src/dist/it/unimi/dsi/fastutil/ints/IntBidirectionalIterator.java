

/* Generic definitions */




/* Assertions (useful to generate conditional code) */
/* Current type and class (and size, if applicable) */
/* Value methods */
/* Interfaces (keys) */
/* Interfaces (values) */
/* Abstract implementations (keys) */
/* Abstract implementations (values) */
/* Static containers (keys) */
/* Static containers (values) */
/* Implementations */
/* Synchronized wrappers */
/* Unmodifiable wrappers */
/* Other wrappers */
/* Methods (keys) */
/* Methods (values) */
/* Methods (keys/values) */
/* Methods that have special names depending on keys (but the special names depend on values) */
/* Equality */
/* Object/Reference-only definitions (keys) */
/* Primitive-type-only definitions (keys) */
/* Object/Reference-only definitions (values) */
/*		 
 * fastutil: Fast & compact type-specific collections for Java
 *
 * Copyright (C) 2002-2008 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package it.unimi.dsi.fastutil.ints;
import it.unimi.dsi.fastutil.BidirectionalIterator;
import it.unimi.dsi.fastutil.objects.ObjectBidirectionalIterator;
/** A type-specific bidirectional iterator; provides an additional method to avoid (un)boxing,
 * and the possibility to skip elements backwards.
 *
 * @see BidirectionalIterator
 */
public interface IntBidirectionalIterator extends IntIterator , ObjectBidirectionalIterator<Integer> {
 /**
	 * Returns the previous element as a primitive type.
	 *
	 * @return the previous element in the iteration.
	 * @see java.util.ListIterator#previous()
	 */
 int previousInt();
 /** Moves back for the given number of elements.
	 *
	 * <P>The effect of this call is exactly the same as that of
	 * calling {@link #previous()} for <code>n</code> times (possibly stopping
	 * if {@link #hasPrevious()} becomes false).
	 *
	 * @param n the number of elements to skip back.
	 * @return the number of elements actually skipped.
	 * @see java.util.Iterator#next()
	 */
 int back( int n );
}
