package edu.toronto.cs.sgb.graphchi;

/**
 * Class for GraphChi edge which contains only predicate id
 */
public class EdgeFBDataMinimized {

	public int predicateid = 0;

	public EdgeFBDataMinimized() {
	}

	@Override
	public String toString() {
		return "ed:" + predicateid ;
	}
}
