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
 * Interface declaration of the reduce phase of the map-reduce
 * algorithm.
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public interface Reducer<K,V>
{
	/**
	 * Reduce function
	 * @param out A {@link OutCollector} that will be used to write output tuples
	 * @param key The key associated to this instance of reducer
	 * @param in An {@link InCollector} containing all the tuples generated
	 * in the map phase for the given key
	 */
	public void reduce(OutCollector<K,V> out, K key, InCollector<K,V> in);
}
