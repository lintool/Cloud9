

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
package it.unimi.dsi.fastutil.objects;
/**  An abstract class facilitating the creation of type-specific {@linkplain java.util.ListIterator list iterators}.
 *
 * <P>This class provides trivial type-specific implementations of {@link
 * java.util.ListIterator#set(Object) set()} and {@link java.util.ListIterator#add(Object) add()} which
 * throw an {@link UnsupportedOperationException}. For primitive types, it also
 * provides a trivial implementation of {@link java.util.ListIterator#set(Object) set()} and {@link
 * java.util.ListIterator#add(Object) add()} that just invokes the type-specific one.
 * 
 *
 * @see java.util.ListIterator
 */
public abstract class AbstractObjectListIterator <K> extends AbstractObjectBidirectionalIterator <K> implements ObjectListIterator <K> {
 protected AbstractObjectListIterator() {}
 /** This method just throws an  {@link UnsupportedOperationException}. */
 public void set( K k ) { throw new UnsupportedOperationException(); }
 /** This method just throws an  {@link UnsupportedOperationException}. */
 public void add( K k ) { throw new UnsupportedOperationException(); }
}
