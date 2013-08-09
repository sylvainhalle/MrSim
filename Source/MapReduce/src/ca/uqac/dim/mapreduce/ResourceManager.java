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

import java.util.LinkedList;
import java.util.List;

/**
 * Coordinates the creation of all threads needed to execute the jobs. This 
 * means that it's the only object able to create mapper Threads and reducer 
 * Threads with all of the informations needed. It's also very important to know
 * there is a management on the number of threads. The manager create a thread,
 * put it in the threads list and send the object to the calling line. 
 * If the maximum of threads has been hit, he don't stop to try to find a 
 * dead thread in the threads list. After that, he erases the dead thread, 
 * add the new thread and return the object to the calling ligne. 
 * Finally, he can check if all threads of the list are dead. The goal is to 
 * make sure of all of the handling is over, before to pass to the other phase.
 * @author Maxime Soucy-Boivin, mastery assistant researcher of Sylvain Hallé
 * @version 1.1
 *
 */
public class ResourceManager<K> {
    
    /**
     * The number of threads by default of the manager. This value is use when 
     * the builder is call without a integer parameter.
     */
    private int threadDefault = 100;
    
    /**
     * The maximum number of threads that can create the manager. The value is 
     * set by the builder.
     */
    private int threadMax = 0;
    
    /**
     * The list that contains all of the created threads of the manager. 
     * It's important to know that the manager is the only one to have acces to 
     * the list.
     */
    private List listThread = new LinkedList();
    
    /**
     * Set the maximum of threads of the manager
     * @param max Value of the maximum
     */
    private void setThreadMax(int max)
    {
        this.threadMax = max;
    }
    
    /**
     * Returns the maximum number of threads of the manager can create
     * @return 
     */
    public int getThreadMax()
    {
        return this.threadMax;
    }
    
    /**
     * Create an instance of ResourceManager with the default value of 
     * threads maximum
     */
    public ResourceManager()
    {
        setThreadMax(threadDefault);
    }
    
    /**
     * Create an instance of ResourceManager
     * @param maxThread Value of the threads maximum given by the user
     */
    public ResourceManager(int maxThread)
    {
        setThreadMax(maxThread);
    }
    
    /**
     * Function that create a mapper thread and return it to the calling line
     * @param t The tuple to analyse
     * @param temp_coll The collector of all results
     * @param m_mapper The {@link Mapper} to use in the map phase
     * @return 
     */
    public Thread getThread(Tuple t, Collector temp_coll, Mapper m_mapper)
    {
        int i=0;
        boolean create = false;
        MapThread MThread = null;
        MapThread ThreadTemp = null;
        
        if(listThread.size() < threadMax)
        {
            MThread = new MapThread(t,temp_coll,m_mapper);
            listThread.add(MThread);
        }
        else
        {
            while(create !=true)
            {
                while(i< listThread.size())
                {
                    ThreadTemp = (MapThread) listThread.get(i);
                
                    if(!ThreadTemp.isAlive())
                    {
                        listThread.remove(i);
                        MThread = new MapThread(t,temp_coll,m_mapper);
                        listThread.add(MThread);
                        create = true;
                        i = listThread.size();
                    }
                    else
                        i++;
                }
                i=0;
            }
        }
        return MThread;
    }
    
    /**
     * Function that create a reducer thread and return it to the calling line
     * @param out The collector of the final results
     * @param key The key to reduce
     * @param s_source The collector of all results of the mapper phase
     * @param m_reducer The {@link Reducer} to use in the reduce phase
     * @return 
     */
    public Thread getThread(Collector out,  K key, Collector s_source, Reducer m_reducer)
    {
        int i=0;
        boolean create = false;
        ReduceThread RThread = null;
        ReduceThread ThreadTemp = null;
        
        if(listThread.size() < threadMax)
        {
           RThread = new ReduceThread(out, key, s_source, m_reducer);
           listThread.add(RThread);
        }
        else
        {
            while(create !=true)
            {
                while(i< listThread.size())
                {
                    ThreadTemp = (ReduceThread) listThread.get(i);

                    if(!ThreadTemp.isAlive())
                    {
                        listThread.remove(i);
                        RThread = new ReduceThread(out, key, s_source, m_reducer);
                        listThread.add(RThread);
                        create = true;
                        i = listThread.size();
                    }
                    else
                        i++;
                }
                i=0;
            }
        }
        return RThread;
    }
    
    /**
     * Function who check if all threads of the list are dead and clear the list
     */
    public void waitThreads()
    {
        int i=0;
        Thread ThreadTemp = null;
        while(i < listThread.size())
        {
            ThreadTemp = (Thread) listThread.get(i);
                
            if(ThreadTemp.isAlive())
                i = 0;    
            else
                i++;
        }
        listThread.clear();
    }
}

/**
 * Class who encapsulates the processing of a mapper and his informations 
 * in a thread
 * @author Maxime Soucy-Boivin, mastery assistant researcher of Sylvain Hallé
 */
class MapThread extends Thread 
{
    /**
     * Informations needed to be transferred to the mapper
     * For more information, see function getThread
     */
    Tuple tThread = new Tuple();
    Collector Thread_Temp_col = new Collector();
    Mapper Thread_m_mapper = null;
    
    /**
     * Create an instance of MapThread
     * @param t The tuple to analyse
     * @param temp_coll The collector of all results
     * @param m_mapper The {@link Mapper} to use in the map phase
     */
    MapThread(Tuple t, Collector temp_coll, Mapper m_mapper) 
    {
        this.tThread = t;
        this.Thread_Temp_col = temp_coll;
        this.Thread_m_mapper = m_mapper;
    }
     
    /**
     * Function who start the execution of the mapper
     */
    public void run() 
    {
        Thread_m_mapper.map(Thread_Temp_col, tThread);
    }
}

/**
 * Class who encapsulates the processing of a reducer and his informations
 * in a thread
 * @author Maxime Soucy-Boivin, mastery assistant researcher of Sylvain Hallé
 */
class ReduceThread<K> extends Thread 
{
    /**
     * Informations needed to be transferred to the reducer
     * For more information, see function getThread
     */
    Collector outThread = new Collector();
    K Thread_key = null;
    Collector Thread_s_source = new Collector();
    Reducer Thread_m_reducer = null;
           
    /**
     * Create an instance of ReduceThread
     * @param out The collector of the final results
     * @param key The key to reduce
     * @param s_source The collector of all results of the mapper phase
     * @param m_reducer The {@link Reducer} to use in the reduce phase
     */
    ReduceThread(Collector out, K key, Collector s_source, Reducer m_reducer) 
    {
        this.outThread = out;
        this.Thread_key = key;
        this.Thread_s_source = s_source;
        this.Thread_m_reducer = m_reducer;
    }
     
   /**
    * Function who start the execution of the reducer
    */
    public void run() 
    {
        Thread_m_reducer.reduce(outThread, Thread_key, Thread_s_source);
    }
}