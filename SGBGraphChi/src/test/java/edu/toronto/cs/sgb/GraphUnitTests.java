/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.toronto.cs.sgb;

import edu.toronto.cs.sgb.graphchi.ByteComparatorUpdate;
import edu.toronto.cs.sgb.graphchi.EdgeFBDataMinimized;
import edu.toronto.cs.sgb.graphchi.EdgeFBDataMinimizedConverter;
import edu.toronto.cs.sgb.graphchi.NodeFBDataMinimized;
import edu.toronto.cs.sgb.graphchi.NodeFBDataMinimizedConverter;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Check saving, restoring, and comparing of node and edge byte-based
 * representations
 */
public class GraphUnitTests {

  static ByteComparatorUpdate bytecomparator;
  static NodeFBDataMinimizedConverter nodedataconverter;
  static EdgeFBDataMinimizedConverter edgedataconverter;

  public GraphUnitTests() {
  }

  @BeforeClass
  public static void setUpClass() {
    nodedataconverter = new NodeFBDataMinimizedConverter();
    edgedataconverter = new EdgeFBDataMinimizedConverter();
    bytecomparator = new ByteComparatorUpdate();
  }

  @AfterClass
  public static void tearDownClass() {
  }

  @Before
  public void setUp() {
    bytecomparator.flagEnableDynamicLength = false;
  }

  @After
  public void tearDown() {
  }

  @Test
  public void edgeTest() {

    final EdgeFBDataMinimized edgedata1 = new EdgeFBDataMinimized();
    edgedata1.predicateid = 1;
    final byte[] edgebytes1 = new byte[edgedataconverter.sizeOf()];
    edgedataconverter.setValue(edgebytes1, edgedata1);
    assertTrue(edgebytes1[0] == 1);
    assertTrue(edgebytes1[1] == 0);
    assertTrue(edgebytes1[2] == 0);
    assertTrue(edgebytes1[3] == 0);

    assertFalse(bytecomparator.compare(edgebytes1, new byte[]{(byte) 0x01}) == 0);

    bytecomparator.flagEnableDynamicLength = true;

    assertTrue(bytecomparator.compare(edgebytes1, new byte[]{(byte) 0x01}) == 0);

    final EdgeFBDataMinimized edgedata2 = new EdgeFBDataMinimized();
    edgedata2.predicateid = 2;
    final byte[] edgebytes2 = new byte[edgedataconverter.sizeOf()];
    edgedataconverter.setValue(edgebytes2, edgedata2);

    assertTrue(bytecomparator.compare(edgebytes1, edgebytes2) < 0);

    final EdgeFBDataMinimized edgedata22 = edgedataconverter.getValue(edgebytes2);

    assertTrue(edgedata2.predicateid == edgedata22.predicateid);
  }

  @Test
  public void nodeTest() {

    final NodeFBDataMinimized nodedata1 = new NodeFBDataMinimized();
    nodedata1.nodeid = 1;
    final byte[] nodebytes1 = new byte[nodedataconverter.sizeOf()];
    nodedataconverter.setValue(nodebytes1, nodedata1);
    assertTrue(nodebytes1[0] == 1);
    assertTrue(nodebytes1[1] == 0);
    assertTrue(nodebytes1[2] == 0);
    assertTrue(nodebytes1[3] == 0);

    assertFalse(bytecomparator.compare(nodebytes1, new byte[]{(byte) 0x01}) == 0);

    bytecomparator.flagEnableDynamicLength = true;

    assertTrue(bytecomparator.compare(nodebytes1, new byte[]{(byte) 0x01}) == 0);

    final NodeFBDataMinimized nodedata2 = new NodeFBDataMinimized();
    nodedata2.nodeid = 2;
    final byte[] nodebytes2 = new byte[nodedataconverter.sizeOf()];
    nodedataconverter.setValue(nodebytes2, nodedata2);

    assertTrue(bytecomparator.compare(nodebytes1, nodebytes2) < 0);

    final NodeFBDataMinimized nodedata22 = nodedataconverter.getValue(nodebytes2);

    assertTrue(nodedata2.nodeid == nodedata22.nodeid);
  }
}
