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
 * Coordinates the execution of a map-reduce job in a single
 * thread. This means that the data source is fed tuple by tuple
 * to the mapper, the output tuples are collected, split
 * according to their keys, and  each list is 
 * sent to the reducer, again in a sequential fashion. As such,
 * the SequentialWorkflow reproduces exactly the processing
 * done by map-reduce, without the distribution of computation. It is
 * best suited to pedagogical and debugging purposes.
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public class SequentialWorkflow<K,V> implements Workflow
{
	private Mapper<K,V> m_mapper = null;
	private Reducer<K,V> m_reducer = null;
	private InCollector<K,V> m_source = null;
	
	/**
	 * Create an instance of SequentialWorkflow.
	 * @param m The {@link Mapper} to use in the map phase
	 * @param r The {@link Reducer} to use in the reduce phase
	 * @param c The {@link InCollector} to use as the input source of tuples
	 */
	public SequentialWorkflow(Mapper<K,V> m, Reducer<K,V> r, InCollector<K,V> c)
	{
		super();
		setMapper(m);
		setReducer(r);
		setSource(c);
	}
	
	public void setMapper(Mapper<K,V> m)
	{
		m_mapper = m;
	}
	
	public void setReducer(Reducer<K,V> r)
	{
		m_reducer = r;
	}
	
	public void setSource(InCollector<K,V> c)
	{
		m_source = c;
	}
	
	public InCollector<K,V> run()
	{
		if (m_mapper == null || m_reducer == null || m_source == null)
			return null;
		assert m_mapper != null;
		assert m_reducer != null;
		assert m_source != null;
		Collector<K,V> temp_coll = new Collector<K,V>();
		m_source.rewind();
		while (m_source.hasNext())
		{
			Tuple<K,V> t = m_source.next();
			m_mapper.map(temp_coll, t);
		}
		Map<K,Collector<K,V>> shuffle = temp_coll.subCollectors();
		Set<K> keys = shuffle.keySet();
		Collector<K,V> out = new Collector<K,V>();
		for (K key : keys)
		{
			Collector<K,V> s_source = shuffle.get(key);
			m_reducer.reduce(out, key, s_source);
		}
		
		return out;
	}

}
