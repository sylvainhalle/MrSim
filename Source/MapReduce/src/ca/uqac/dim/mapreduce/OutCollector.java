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

/**
 * Data source used as the output of the map and reduce 
 * phases. An OutCollector can be used to
 * store data tuples using the {@link OutCollector#collect(Tuple)}
 * method.
 *
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public interface OutCollector<K,V>
{
	/**
	 * Add a new tuple to the Collector
	 * @param t The {@link Tuple} to add
	 */
	public void collect(Tuple<K,V> t);
	
	/**
	 * Rewinds the collector to the beginning of its enumeration
	 */
	public void rewind();
}
