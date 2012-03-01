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
 * Coordinates the execution of a map-reduce job.
 * @author Sylvain Hallé
 * @version 1.1
 *
 */
public interface Workflow<K,V>
{
	/**
	 * Start a map-reduce job and output the results as a single
	 * Collector containing all output tuples.
	 * @return
	 */
	public InCollector<K,V> run();
}
