package edu.toronto.cs.sgb.graphchi;

import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import edu.cmu.graphchi.datablocks.IntConverter;

/**
 * GraphChi converter for edges
 */
public class EdgeFBDataMinimizedConverter implements BytesToValueConverter<EdgeFBDataMinimized> {

	final static IntConverter ic = new IntConverter();

	// 4 bytes for id
	// 16 bytes for source hash
	// 16 bytes for target hash
	@Override
	public int sizeOf() {
//		return 4 + (16 * 4);
		return 4;
	}

	// format - [0..3]idbytes, [4..16+4]sourcehashbytes, [16+4..16+16+4]targethashbytes
	// int id takes 4 LSB
	// 16 bytes for source hash
	// 16 bytes for target hash
	@Override
	public EdgeFBDataMinimized getValue(byte[] array) {
		final EdgeFBDataMinimized ret = new EdgeFBDataMinimized();
		// get id
		ret.predicateid = ic.getValue(array);

//		System.arraycopy(array, 4, ret.targethash1, 0, 16);
//		System.arraycopy(array, 4 + 16, ret.targethash2, 0, 16);
//		System.arraycopy(array, 4 + (16 * 2), ret.sourcehash1, 0, 16);
//		System.arraycopy(array, 4 + (16 * 3), ret.sourcehash2, 0, 16);

		return ret;
	}

	@Override
	public void setValue(byte[] array, EdgeFBDataMinimized val) {

		// set the 4 LSB with the int id bytes
		ic.setValue(array, val.predicateid);

//		System.arraycopy(val.targethash1, 0, array, 4, 16);
//		System.arraycopy(val.targethash2, 0, array, 4 + 16, 16);
//		System.arraycopy(val.sourcehash1, 0, array, 4 + (16 * 2), 16);
//		System.arraycopy(val.sourcehash2, 0, array, 4 + (16 * 3), 16);

	}
}
