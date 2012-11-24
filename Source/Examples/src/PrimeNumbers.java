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
import ca.uqac.dim.mapreduce.*;

/**
 * Demonstration of MapReduce processing with MrSim. This program
 * computes the first 1,000 prime numbers using a map-reduce algorithm.
 * @author Sylvain Hallé
 */
public class PrimeNumbers
{
	public static void main(String[] args)
	{
		SequentialWorkflow<Integer,Integer> w = 
		    new SequentialWorkflow<Integer,Integer>(
		        new PrimeMap(), // Mapper
		        new PrimeReduce(), // Reducer
		        new PrimeCollector() // Input collector
		        );
		// Run the workflow
		InCollector<Integer,Integer> results = w.run();
		// Iterate over InCollector to display results
		System.out.println(results);
	}
	
	/**
	 * Create the initial tuples to be sent to the map-reduce job.
	 * This collector will contain tuples of the form
	 * (<i>x</i>,<i>y</i>), with <i>x</i> and <i>y</i> all combination
	 * of integers between 1 and 1,000.
	 */
	private static class PrimeCollector extends Collector<Integer,Integer>
	{
		/*package*/ PrimeCollector()
		{
			super();
			for (int i = 1; i <= 1000; i++)
			{
				for (int j = 1; j <= 1000; j++)
				{
					Tuple<Integer,Integer> t = new Tuple<Integer,Integer>(
					    new Integer(i), new Integer(j));
					super.collect(t);
				}
			}
		}
	}
	
	/**
	 * Implementation of the mapper.
	 * <ol>
	 * <li>Input: a tuple (<i>x</i>,<i>y</i>), with <i>x</i> and
	 *   <i>y</i> integers</li>
	 * <li>Output: the same tuple (<i>x</i>,<i>y</i>), only if
	 *   <i>y</i> divides <i>x</i>
	 * (and nothing otherwise)</li>
	 * </ol>
	 * @author Sylvain Hallé
	 *
	 */
	private static class PrimeMap implements Mapper<Integer,Integer>
	{
		@Override
		public void map(OutCollector<Integer,Integer> out, Tuple<Integer,Integer> t)
		{
			int i = t.getKey().intValue();
			int j = t.getValue().intValue();
			if (i % j == 0)
				out.collect(t);
		}
	}
	
	/**
	 * Implementation of the reducer.
	 * <ol>
	 * <li>Input: tuple (<i>x</i>,<i>y</i>), with <i>x</i> and <i>y</i> 
	 *   integers and <i>y</i> divides <i>x</i></li>
	 * <li>Output: the tuple (<i>x</i>,1), if the only values seen for
	 *   <i>y</i> are 1 and <i>x</i> (and nothing otherwise)</li>
	 * </ol>
	 * @author Sylvain Hallé
	 *
	 */
	private static class PrimeReduce implements Reducer<Integer,Integer>
	{
		@Override
		public void reduce(OutCollector<Integer,Integer> out, Integer key, 
		    InCollector<Integer,Integer> in)
		{
			boolean ok = true;
			int i = key.intValue();
			while (in.hasNext() && ok)
			{
				Tuple<Integer,Integer> t = in.next();
				int j = t.getValue().intValue();
				if (j != 1 && j != i)
					ok = false;
			}
			if (ok)
				out.collect(new Tuple<Integer,Integer>(key, new Integer(1)));
		}
	}
}
