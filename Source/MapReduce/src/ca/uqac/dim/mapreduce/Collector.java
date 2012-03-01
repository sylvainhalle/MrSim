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
import java.util.*;

/**
 * Data source used both as the input and output of the map and reduce 
 * phases. A Collector can be used to:
 * <ol>
 * <li>Store data tuples using the {@link collect} method</li>
 * <li>Enumerate data tuples using the {@link hasNext} and {@link next}
 * methods, like an {@link Iterator}</li>
 * <li>Partition the set of tuples into a set of Collectors, grouping
 * tuples by their key, using the {@link subCollector} and 
 * {@link subCollectors} methods</li> 
 * </ol>
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public class Collector<K,V> implements InCollector<K,V>, OutCollector<K,V>
{
	private List<Tuple<K,V>> m_tuples = new LinkedList<Tuple<K,V>>();
	private Iterator<Tuple<K,V>> m_it = null;
	
	/**
	 * Return the Collector's contents as a list of tuples
	 * @return The list of tuples
	 */
	public List<Tuple<K,V>> toList()
	{
		return m_tuples;
	}
	
	/**
	 * Add a collection of tuples to the Collector
	 * @param list A collection of {@link Tuple}
	 */
	public void addAll(Collection<Tuple<K,V>> list)
	{
		m_tuples.addAll(list);
	}
	
	/**
	 * Add a new tuple to the Collector
	 * @param t The {@link Tuple} to add
	 */
	public void collect(Tuple<K,V> t)
	{
		m_tuples.add(t);
	}
	
	/**
	 * Returns a new Collector whose content is made of all tuples with
	 * given key
	 * @param key The key to find
	 * @return A new {@link Collector}
	 */
	public Collector<K,V> subCollector(K key)
	{
		Collector<K,V> c = new Collector<K,V>();
		for (Tuple<K,V> t : m_tuples)
		{
			if (t.getKey().equals(key))
				c.m_tuples.add(t);
		}
		return c;
	}
	
	public int count()
	{
		return m_tuples.size();
	}
	
	/**
	 * Partitions the set of tuples into new collectors, each containing all
	 * tuples with the same key
	 * @return A map from keys to Collectors
	 */
	public Map<K,Collector<K,V>> subCollectors()
	{
		Map<K,Collector<K,V>> out = new HashMap<K,Collector<K,V>>();
		for (Tuple<K,V> t : m_tuples)
		{
			K key = t.getKey();
			Collector<K,V> c = out.get(key);
			if (c == null)
				c = new Collector<K,V>();
			c.collect(t);
			out.put(key, c);
		}
		return out;
	}

	@Override
	public boolean hasNext()
	{
		if (m_it == null)
			m_it = m_tuples.iterator();
		return m_it.hasNext();
	}

	@Override
	public Tuple<K,V> next()
	{
		return m_it.next();
	}

	@Override
	public void remove()
	{
		m_it.remove();
	}
	
	@Override
	public String toString()
	{
		return m_tuples.toString();
	}

	@Override
	public void rewind()
	{
		m_it = null;
	}
}
