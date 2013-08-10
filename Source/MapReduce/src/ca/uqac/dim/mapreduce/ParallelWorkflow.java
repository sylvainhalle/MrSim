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
import java.util.Date;

/**
 * Coordinates the execution of a map-reduce job in a multithreading 
 * mode. This means that the data source is split tuple by tuple and 
 * each tuple has it's own mapper (thread). After that, the output 
 * tuples are collected, split according to their keys, and  each 
 * list is sent to his reducer (thread).  As such, the ParallelWorkflow 
 * reproduces exactly the processing done by map-reduce, 
 * with the distribution of computation. It is best suited to 
 * pedagogical and debugging purposes.
 * @author Maxime Soucy-Boivin
 * @version 1.1
 * 
 */
public class ParallelWorkflow<K,V> implements Workflow<K,V>
{
    private Mapper<K,V> m_mapper = null;
	private Reducer<K,V> m_reducer = null;
	private InCollector<K,V> m_source = null;
    private ResourceManager<K,V> m_managerMapper = null;
    private ResourceManager<K,V> m_managerReducer = null;
	
	/**
	 * The total number of tuples that the mappers will produce.
	 * This is only necessary for gathering statistics, and is not
	 * required in the MapReduce processing <i>per se</i>.
	 */
	protected long m_totalTuples = 0;
	
	/**
	 * The maximum number of tuples that a single reducer will process.
	 * This is used as a measure of the "linearity" of the MapReduce job:
	 * assuming all reducers worked on a separate thread, this value is
	 * proportional to the time the longest reducer would take.
	 * Intuitively, the ratio maxTuples/totalTuples indicates the "speedup"
	 * incurred by the use of parallel reducers compared to a strictly
	 * linear processing. 
	 */
	protected long m_maxTuples = 0;
	
        public ParallelWorkflow(Mapper<K,V> m, Reducer<K,V> r, InCollector<K,V> c)
        {
            super();
            setMapper(m);
            setReducer(r);
            setSource(c);
            
            ResourceManager<K,V> rmM = new ResourceManager<K,V>();
            setManagerMapper(rmM);
            
            ResourceManager<K,V> rmR = new ResourceManager<K,V>();
            setManagerReducer(rmR);
        }
	/**
	 * Create an instance of ParallelWorkflow.
	 * @param m The {@link Mapper} to use in the map phase
	 * @param r The {@link Reducer} to use in the reduce phase
	 * @param c The {@link InCollector} to use as the input source of tuples
         * @param rmM The {@link ResourceManager} to use as a manager of threads for the Map phase
         * @param rmR The {@link ResourceManager} to use as a manager of threads for the Reduce phase
	 */
    public ParallelWorkflow(Mapper<K,V> m, Reducer<K,V> r, InCollector<K,V> c, ResourceManager<K,V> rmM, ResourceManager<K,V> rmR )
	{
		super();
		setMapper(m);
		setReducer(r);
		setSource(c);
                setManagerMapper(rmM);
                setManagerReducer(rmR);
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
        
    public void setManagerMapper(ResourceManager<K,V> rmM)
    {
        m_managerMapper = rmM;
    }
        
    public void setManagerReducer(ResourceManager<K,V> rmR)
    {
        m_managerReducer = rmR;
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
            
            long timeBeforeMap = new Date().getTime();
            
            while (m_source.hasNext())
            {
                Tuple<K,V> t = m_source.next();
                
                //Thread for all mappers
                Thread MThread = m_managerMapper.getThread(t, temp_coll, m_mapper);
                MThread.start();
            }
            //WAIT all mapper theads to finish
            m_managerMapper.waitThreads();
            
            long timeAfterMap = new Date().getTime();
            long timePhaseMap = timeAfterMap - timeBeforeMap;
            System.out.println("--------------------------------------------------------");
            System.out.println("------------------Time of every Phases------------------");
            System.out.println("--------------------------------------------------------");
            System.out.println("                  Map : " + timePhaseMap + " Milliseconds");
        
            Map<K,Collector<K,V>> shuffle = temp_coll.subCollectors();
            Set<K> keys = shuffle.keySet();
            Collector<K,V> out = new Collector<K,V>();
            
            long timeBeforeReduce = new Date().getTime();
            for (K key : keys)
            {
                Collector<K,V> s_source = shuffle.get(key);
                int num_tuples = s_source.count();
                m_totalTuples += num_tuples;
                m_maxTuples = Math.max(m_maxTuples, num_tuples);
                
                //Tread for all Reducers
                Thread RThread = m_managerReducer.getThread(out, key, s_source, m_reducer);
                RThread.start();
            }
            
            //WAIT all reduce theads to finish
             m_managerReducer.waitThreads();
             
             long timeAfterReduce = new Date().getTime();
             long timePhaseReduce = timeAfterReduce - timeBeforeReduce;
             long timePhaseTotal = timePhaseMap + timePhaseReduce;
             double timeSeconds = (double) timePhaseTotal/1000;
             int timeMinutes = (int) (timePhaseTotal/1000)/60;
             double timeSecMins = ( (double)timePhaseTotal/1000) - ( (double)timeMinutes * 60);
             
             System.out.println("               Reduce : " + timePhaseReduce + " Milliseconds");
             System.out.println("--------------------------------------------------------");
             System.out.println("                Total : " + timePhaseTotal + " Milliseconds");
             System.out.println("        Total Seconds : " + timeSeconds);
             System.out.println("        Total Minutes : " + timeMinutes + "." + timeSecMins);
             System.out.println("--------------------------------------------------------");
            return out;
        }
        
        /**
	 * Returns the maximum number of tuples processed by a single
	 * reducer in the process. This method returns 0 if the MapReduce
	 * job hasn't executed yet (i.e. you should call it only after
	 * a call to {@link run}).
	 * @return
	 */
	public long getMaxTuples()
	{
		return m_maxTuples;
	}
	
	/**
	 * Returns the total number of tuples processed by all reducers.
	 * This method returns 0 if the MapReduce
	 * job hasn't executed yet (i.e. you should call it only after
	 * a call to {@link run}).
	 * @return
	 */
	public long getTotalTuples()
	{
		return m_totalTuples;
	}
}