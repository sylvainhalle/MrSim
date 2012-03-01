import ca.uqac.dim.mapreduce.*;

/**
 * Compute the first 1,000 prime numbers using a map-reduce algorithm.
 * @author Sylvain Hallé
 *
 */
public class PrimeNumbers
{
	public static void main(String[] args)
	{
		SequentialWorkflow<Integer,Integer> w = new SequentialWorkflow<Integer,Integer>(new PrimeMap(), new PrimeReduce(), new PrimeCollector());
		InCollector<Integer,Integer> results = w.run();
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
					Tuple<Integer,Integer> t = new Tuple<Integer,Integer>(new Integer(i), new Integer(j));
					super.collect(t);
				}
			}
		}
	}
	
	/**
	 * Map class.
	 * <ol>
	 * <li>Input: a tuple (<i>x</i>,<i>y</i>), with <i>x</i> and <i>y</i> integers</li>
	 * <li>Output: the same tuple (<i>x</i>,<i>y</i>), only if <i>y</i> divides <i>x</i>
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
	 * Reduce class.
	 * <ol>
	 * <li>Input: tuple (<i>x</i>,<i>y</i>), with <i>x</i> and <i>y</i> integers and
	 * <i>y</i> divides <i>x</i></li>
	 * <li>Output: the tuple (<i>x</i>,1), if the only values seen for <i>y</i> are
	 * 1 and <i>x</i>
	 * (and nothing otherwise)</li>
	 * </ol>
	 * @author Sylvain Hallé
	 *
	 */
	private static class PrimeReduce implements Reducer<Integer,Integer>
	{
		@Override
		public void reduce(OutCollector<Integer,Integer> out, Integer key, InCollector<Integer,Integer> in)
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
