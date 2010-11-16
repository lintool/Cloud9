

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
package it.unimi.dsi.fastutil.longs;
/**  An abstract class facilitating the creation of type-specific iterators.
 *
 * <P>To create a type-specific iterator you need both a method returning the
 * next element as primitive type and a method returning the next element as an
 * object. However, if you inherit from this class you need just one (anyone).
 *
 * <P>This class implements also a trivial version of {@link #skip(int)} that uses
 * type-specific methods; moreover, {@link #remove()} will throw an {@link
 * UnsupportedOperationException}.
 *
 * @see java.util.Iterator
 */
public abstract class AbstractLongIterator implements LongIterator {
 protected AbstractLongIterator() {}
 /** Delegates to the corresponding generic method. */
 public long nextLong() { return next().longValue(); }
 /** Delegates to the corresponding type-specific method. */
 public Long next() { return Long.valueOf( nextLong() ); }
 /** This method just throws an  {@link UnsupportedOperationException}. */
 public void remove() { throw new UnsupportedOperationException(); }
 /** This method just iterates the type-specific version of {@link #next()} for at most
	 * <code>n</code> times, stopping if {@link #hasNext()} becomes false.*/
 public int skip( final int n ) {
  int i = n;
  while( i-- != 0 && hasNext() ) nextLong();
  return n - i - 1;
 }
}
