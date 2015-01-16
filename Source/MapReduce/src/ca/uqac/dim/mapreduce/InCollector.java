/*
    A basic map-reduce implementation
    Copyright (C) 2011 Sylvain Hallé

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package ca.uqac.dim.mapreduce;

import java.util.Iterator;

/**
 * Data source used as the input of the map and reduce 
 * phases. An InCollector can be used to enumerate data
 * tuples using the {@link Collector#hasNext()} and
 * {@link Collector#next()} methods, like an {@link Iterator}.
 * 
 * @author Sylvain Hallé
 * @version 1.0
 *
 */
public interface InCollector<K,V> extends Iterator<Tuple<K,V>>
{
	/**
	 * Count the number of tuples in the collector
	 * @return The number of tuples. A Collector for which
	 * the size cannot be computed should return -1.
	 */
	public int count();
	
	/**
	 * Rewinds the collector to the beginning of its enumeration
	 */
	public void rewind();
}
