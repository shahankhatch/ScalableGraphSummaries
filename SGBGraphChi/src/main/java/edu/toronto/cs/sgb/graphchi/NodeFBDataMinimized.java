package edu.toronto.cs.sgb.graphchi;

/**
 * Node for GraphChi which contains only node id
 */
public class NodeFBDataMinimized {

	public int nodeid = -1;

	public NodeFBDataMinimized() {
	}

	@Override
	public String toString() {
		return "nd:" + nodeid ;
	}
}
