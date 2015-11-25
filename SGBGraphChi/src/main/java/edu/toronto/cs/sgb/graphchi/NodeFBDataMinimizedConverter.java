package edu.toronto.cs.sgb.graphchi;

import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.IntConverter;

/**
 * GraphChi converter for nodes
 */
public class NodeFBDataMinimizedConverter implements BytesToValueConverter<NodeFBDataMinimized> {

	final static IntConverter ic = new IntConverter();

	// 4 byte id
	// 16 byte hash
	@Override
	public int sizeOf() {
//		return 4 + 16 + 16;
		return 4;
	}

	@Override
	public NodeFBDataMinimized getValue(byte[] array) {
		final NodeFBDataMinimized ret = new NodeFBDataMinimized();

		// get the id
		ret.nodeid = ic.getValue(array);

//		System.arraycopy(array, 4, ret.h, 0, 16);
//		System.arraycopy(array, 4 + 16, ret.h_old, 0, 16);

		return ret;
	}

	@Override
	public void setValue(byte[] array, NodeFBDataMinimized val) {
		// set the 4 LSB with the int id bytes
		ic.setValue(array, val.nodeid);

		// set the target hash bytes
//		System.arraycopy(val.h, 0, array, 4, 16);
//		System.arraycopy(val.h_old, 0, array, 4 + 16, 16);
	}
}
