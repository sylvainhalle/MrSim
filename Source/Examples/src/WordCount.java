import java.io.*;
import ca.uqac.dim.mapreduce.*;

/**
 * Count word occurrences in Franz Kafka's novel
 * <cite>The Metamorphosis</cite>.
 * @author Sylvain Hallé
 *
 */
public class WordCount
{
	public static void main(String[] args)
	{
		int k = 4; // We keep only words with at least k letters
		int n = 50; // We keep only words that appear n times or more
		SequentialWorkflow<String,String> w = new SequentialWorkflow<String,String>(new CountMap(k), new CountReduce(n), new BigStringCollector("pg5200.txt"));
		InCollector<String,String> results = w.run();
		System.out.println("There are " + results.count() + " word(s) of at least " + k + " letter(s) that appear at least " + n + " times");
		System.out.println(results);
	}
	
	/**
	 * Create the initial tuples to be sent to the map-reduce, which
     * contains a single tuples of the form
	 * (<i>w</i>, ""), for <i>w</i> the whole contents of the file.
	 */
	private static class BigStringCollector extends Collector<String,String>
	{
		/**
		 * Creates a WordCollector from an input text file. Words
		 * are separated by spaces or new lines.
		 * @param filename The file to read
		 */
		/*package*/ BigStringCollector(String filename)
		{
			super();
			StringBuffer sb = new StringBuffer();
			try
			{
				BufferedReader input =  new BufferedReader(new FileReader(filename));
				try
				{
					String line = null;
					while (( line = input.readLine()) != null)
					{
						sb.append(line.trim()).append(" ");
					}
				}
				finally
				{
					input.close();
				}
			}
			catch (IOException ex)
			{
				ex.printStackTrace();
			}
			super.collect(new Tuple<String,String>(sb.toString(), ""));
		}
	}
	
	/**
	 * Map class.
	 * <ol>
	 * <li>Input: a tuple (<i>w</i>,""), with <i>w</i> the contents of a file
	 * (i.e. a long string)</li>
	 * <li>Output: the tuple (<i>w</i>,1), where <i>w</i> is each word
	 * in the text, only if it has at least <i>k</i> letters</li>
	 * </ol>
	 * @author Sylvain Hallé
	 *
	 */
	private static class CountMap implements Mapper<String,String>
	{
		private int m_minLetters = 1;
		
		/**
		 * Constructs a mapper and sets the minimum number of letters
		 * required to retain a tuple. This number is the <i>k</i> in the description
		 * above.
		 * @param k Minimum number of letters in the key to output a tuple
		 */
		/*package*/ CountMap(int k)
		{
			m_minLetters = k;
		}
		
		@Override
		public void map(OutCollector<String,String> out, Tuple<String,String> t)
		{
			String[] words = t.getKey().split(" ");
			for (String w : words)
			{
				// Remove punctuation and convert to lowercase
				String new_w = w.toLowerCase();
				new_w = new_w.replaceAll("[^\\w]", "");
				if (new_w.length() >= m_minLetters)
					out.collect(new Tuple<String,String>(new_w, "1"));
			}
		}
	}
	
	/**
	 * Reduce class.
	 * <ol>
	 * <li>Input: tuples (<i>w</i>,1), with <i>w</i> some word</li>
	 * <li>Output: the tuple (<i>w</i>,<i>x</i>), with <i>x</i> the number of tuples
	 * passed as input, only if <i>x</i> is greater than some threshold
	 * value <i>n</i></li>
	 * </ol>
	 * anagram.
	 * @author Sylvain Hallé
	 *
	 */
	private static class CountReduce implements Reducer<String,String>
	{
		private int m_numOccurrences = 2;
		
		/**
		 * Constructs a reducer and sets the number of word occurrences
		 * required to retain a tuple. This number is the <i>n</i> in the description
		 * above.
		 * @param n Minimum number of word occurrences to output a tuple
		 */
		/*package*/ CountReduce(int n)
		{
			m_numOccurrences = n;
		}
		
		@Override
		public void reduce(OutCollector<String,String> out, String key, InCollector<String,String> in)
		{
			int num_words = in.count();
			if (num_words >= m_numOccurrences)
				out.collect(new Tuple<String,String>(key, "" + num_words));
		}
	}

}
