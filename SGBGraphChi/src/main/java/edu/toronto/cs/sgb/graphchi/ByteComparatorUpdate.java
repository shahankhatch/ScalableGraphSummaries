package edu.toronto.cs.sgb.graphchi;

import java.util.Comparator;

/**
 * Byte-based comparator. Can handle two arrays of different lengths if flag is
 * enabled
 */
public class ByteComparatorUpdate implements Comparator<byte[]> {

  // feature that allows comparing byte arrays of different length
  public boolean flagEnableDynamicLength = false;

  @Override
  public int compare(byte[] left, byte[] right) {
    // check the that the values 
    final int minlength = Math.min(left.length, right.length);
    for (int i = 0; i < minlength; i++) {
      // the 0xff is to ensure that are not two's complement
      int a = (left[i] & 0xff);
      int b = (right[i] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    // values thus far have been equal (based on the smaller byte array)
    // check if the arrays need to be the same length
    if (!flagEnableDynamicLength) {
      return right.length - left.length;
    }
    // if remainder bytes in larger array are non-zero
    // then it should be placed after the smaller array
    // need to remember the larger array to return correct compare value
    final byte[] toCheck = (left.length > right.length) ? left : right;
    boolean isLeft = (toCheck.length == left.length);
    for (int i = minlength + 1; i < Math.max(left.length, right.length); i++) {
      if ((toCheck[i] & 0xff) != 0) {
        // if there is a non-zero in left then it is larger than right
        // otherwise left should go before right
        return (isLeft) ? 1 : -1;
      }
    }
    // at this point all the remainder of the larger array are zero
    // so they are equal
    return 0;
  }

}
