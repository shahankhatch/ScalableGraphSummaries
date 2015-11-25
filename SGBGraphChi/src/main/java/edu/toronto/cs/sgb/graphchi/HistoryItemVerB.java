package edu.toronto.cs.sgb.graphchi;

/**
 * Statistics for partition refinement for each iteration
*/
public class HistoryItemVerB {
	public int iteration;
	public boolean dir_F;
	public boolean dir_B;
	public int value;
	public double time_s;
	public int singletonNodeCount;
	public int stableNodeCount;
	public int stableBlockCount;
	public int stableSingletonCount;

	public static String schema() {
		return "iteration,dir,singletonNodeCount,stableNodeCount,stableBlockCount,stableSingletonCount,time_s";
	}
	
	@Override
	public String toString() {
		return "<" + iteration + "," + (dir_F ? "F" : "") + (dir_B ? "B" : "") + "," + value + ","+singletonNodeCount+","+stableNodeCount+","+stableBlockCount+","+stableSingletonCount+","+time_s+">";
	}
}
