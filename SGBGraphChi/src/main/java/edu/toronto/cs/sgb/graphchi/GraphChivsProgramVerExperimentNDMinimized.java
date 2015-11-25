package edu.toronto.cs.sgb.graphchi;

import edu.cmu.graphchi.ChiEdge;
import edu.cmu.graphchi.ChiVertex;
import edu.cmu.graphchi.GraphChiContext;
import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.toronto.cs.sgb.util.FileUtil;
import edu.toronto.cs.sgb.util.SimpleLogger;
import edu.toronto.cs.sgb.util.Timer;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;
import java.util.function.BiConsumer;
import org.apache.commons.codec.digest.DigestUtils;
import org.javatuples.Triplet;
import org.mapdb.Bind;
import org.mapdb.DB;
import org.mapdb.Fun;
import org.mapdb.Hasher;
import org.mapdb.Serializer;

public class GraphChivsProgramVerExperimentNDMinimized implements GraphChiProgram<NodeFBDataMinimized, EdgeFBDataMinimized> {

  // fw-bisim optimization that skips nodes with no out-edges
  final private boolean flagSkipNoOutEdgesForFWOnly = false;

  // bisim optimization that skips nodes in singleton blocks
  static public boolean flagSkipSingletons = false;
  // must be enabled with flagTrackStability=true to really make sense 
  static public boolean flagCountSingletons = false;

  // count block sizes 
  final private boolean flagCountBlockSizes = false;

  // print summary as RDF?
  final private boolean flagDoPrint = false;
  public static boolean flagTrackRefinementSet = false;

  // enabled only if printing, skips printing and instead just counts
  private final boolean flagCountEdgesOnly = false;
  public final static Object summaryCountedEdgesLock = new Object();
  public static int summaryCountedEdges = 0;

  private final boolean flagCountProcessedInstanceEdges = false;
  private final static Object countProcessedInstanceEdgesLock = new Object();
  private static int countProcessedInstanceEdges = 0;

  // untested?
  private final boolean flagPrintMDL = false;

  // flag to track number of stable blocks in each iteration
  final private boolean flagTrackStability = false;
  final private boolean flagSkipStable = false;
  int numstable;
  int numstablesingleton;
  private final Object numstableLock = new Object();
  private final Object numstablesingletonLock = new Object();
  private Set<byte[]> stableBlocks;

  public static boolean dir_F = true;
  public static boolean dir_B = true;

  Timer itertimer;

  // manually synchronized vertex count
  int numvertices;
  private final Object numverticesLock = new Object();

  final private DB DBmaker;

  // partitions and respective block sizes
  // TODO: encapsulate these objects within their respective history object
  private Map<Integer, byte[]> currentPartition;
  private Map<Integer, byte[]> previousPartitionKMinusOne;
  private Map<Integer, byte[]> previousPartitionKMinusTwo;

  private Map<byte[], Integer> MDB_currentPartitionBlockSizes;
  private Map<byte[], Integer> MDB_KMinusOnePartitionBlockSizes;
  private Map<byte[], Integer> MDB_KMinusTwoPartitionBlockSizes;

  private final Object singletonCountLock = new Object();
  private int singletonCount = 0;

  // non-thread-safe bit sets
  // The current iteration's nodes are not disabled until the end of the iteration
  // since a uniform view is needed by each node to compute block ids.
  // Disabling it during an iteration could create different signatures
  // for nodes in the same block.
  // nodes to disable at end of current iteration
  private final BitSet bs_toDisable = new BitSet();
  // nodes disabled prior to start of current iteration
  private final BitSet bs_allDisabled = new BitSet();

  // GraphChi engine
  private final GraphChiEngine engine;

  // object to lock the scheduler
  final private Object schedulerLock = new Object();

  // map and reverse-map between node strings and node ids (off-heap using MapDB)
  private final Map<String, Integer> nodeIds;
  final public NavigableSet<Fun.Tuple2<Integer, String>> inv_nodes;

  // map and reverse-map between predicate strings and node ids (on-heap using POJO)
  private final Map<String, Integer> predIds;
  private final HashMap<Integer, String> inv_preds2;

  // keep track of blocks that have been printed already
  private final Set<byte[]> printedBlocks;
  // record partitions and their splits
  private Set<String> refinementSet;

  // writer for summary edges
  private PrintWriter summaryEdgesWriter;

  // flag to indicate that the last iteration has been encountered
  // it is only true at the end of two consecutive iterations having 
  // the same number of distinct block ids in the their partition
  private boolean flagLastIteration = false;

  // logger
  private SimpleLogger simplelogger;

  GraphChivsProgramVerExperimentNDMinimized(DB DBmaker, GraphChiEngine<NodeFBDataMinimized, EdgeFBDataMinimized> engine, SimpleLogger simplelogger, Map<String, Integer> nodeIds, Map<String, Integer> predIds) {
    this.engine = engine;
    this.DBmaker = DBmaker;
    this.simplelogger = simplelogger;

    this.nodeIds = nodeIds;
    this.predIds = predIds;

    this.inv_preds2 = new HashMap<>();
    this.predIds.forEach(new BiConsumer<String, Integer>() {

      @Override
      public void accept(String t, Integer u) {
        inv_preds2.put(u, t);
      }
    });

    inv_nodes = DBmaker.createTreeSet("nodes_inverse").counterEnable().make();
    Bind.mapInverse((Bind.MapWithModificationListener) this.nodeIds, inv_nodes);

    printedBlocks = DBmaker.createHashSet("printedBlocks").serializer(Serializer.BYTE_ARRAY).hasher(Hasher.BYTE_ARRAY).make();

    refinementSet = DBmaker.createHashSet("refinementSet").make();

    stableBlocks = DBmaker.createHashSet("stableBlocks0").counterEnable().hasher(Hasher.BYTE_ARRAY).serializer(Serializer.BYTE_ARRAY).make();

    currentPartition = DBmaker.createHashMap("currentpartition-V1").valueSerializer(Serializer.BYTE_ARRAY).make();
    previousPartitionKMinusOne = DBmaker.createHashMap("currentpartition-V2").valueSerializer(Serializer.BYTE_ARRAY).make();
    previousPartitionKMinusTwo = DBmaker.createHashMap("currentpartitionKminus2-V2").valueSerializer(Serializer.BYTE_ARRAY).make();

    MDB_currentPartitionBlockSizes = DBmaker.createHashMap("blocksizesnext00").counterEnable().hasher(Hasher.BYTE_ARRAY).keySerializer(Serializer.BYTE_ARRAY).make();
    MDB_KMinusOnePartitionBlockSizes = DBmaker.createHashMap("blocksizesnext01").counterEnable().hasher(Hasher.BYTE_ARRAY).keySerializer(Serializer.BYTE_ARRAY).make();
    MDB_KMinusTwoPartitionBlockSizes = DBmaker.createHashMap("blocksizesnexti02").counterEnable().hasher(Hasher.BYTE_ARRAY).keySerializer(Serializer.BYTE_ARRAY).make();

    if (flagDoPrint) {
      try {
        summaryEdgesWriter = FileUtil.getPrintWriter("summaryEdges.txt.gz");
      } catch (IOException ex) {
        ex.printStackTrace();
        System.exit(1);
      }
    }

  }

  @Override
  public void update(final ChiVertex<NodeFBDataMinimized, EdgeFBDataMinimized> vertex, final GraphChiContext context) {
     // this method is the one that is called by GraphChi's BGP processing

    // initialize values in the first iteration
    if (context.getIteration() == 0) {
      synchronized (numverticesLock) {
        numvertices++;
      }
    }

    // current processed vertex id
    final int vertexid;
    vertexid = vertex.getId();

    // vertex's block id from previous iteration
    final byte[] bhere;
    bhere = getPreviousBlockId(vertexid);

    try {
      // print in the last iteration
      if (flagLastIteration && flagDoPrint) {
        synchronized (printedBlocks) {
          if (printedBlocks.contains(bhere)) {
            return;
          }
          printedBlocks.add(bhere);
        }
        printSummaryEdges(context, vertex, bhere);

        // no need to execute update function to update hash
        return;
      }

      // FW summary optimization that skips nodes with no outedges since they all fall into the same block
      if (flagSkipNoOutEdgesForFWOnly && (vertex.numOutEdges() == 0 && dir_B == false && dir_F == true)) {
        synchronized (bs_toDisable) {
          bs_toDisable.set(vertexid);
        }
        setNodeBlockId(vertexid, bhere);
        return;
      }

      // Singleton optimization that skips nodes that are in a singleton block
      if ((flagSkipSingletons || flagCountSingletons) && context.getIteration() > 0) {
        final int bheresize;
        bheresize = MDB_KMinusOnePartitionBlockSizes.get(bhere);
        if (bheresize == 1) {

          synchronized (singletonCountLock) {
            singletonCount++;
          }

          if (flagDoPrint && !GraphChiVSMainVerExperimentNDMinimized.flagIntIn) {
            printSummaryEdges(context, vertex, bhere);
            printedBlocks.add(bhere);
          }
          if (flagSkipSingletons) {
            bs_toDisable.set(vertexid);
            setNodeBlockId(vertexid, bhere);
            return;
          }
        }

      }

      context.getScheduler().addTask(vertexid);

      // create the object that gives MD5 hashes
      final MessageDigest md = MessageDigest.getInstance("MD5");

      // hashset from which to generate a canonical ordering to then generate a signature
      final Set<ByteBuffer> hs = new HashSet<>();

      // flag to determine node stability
      // TODO: put this in a descriptor that encapsulates nodes
      boolean flagNodeStable = true;

      // gather
      if (dir_F) {
        // fw
        for (int i = 0; i < vertex.numOutEdges(); i++) {
          final int outEdgeVertexId = vertex.getOutEdgeId(i);
          if (bs_allDisabled.get(outEdgeVertexId)) {
            continue;
          }
          if (flagCountProcessedInstanceEdges) {
            synchronized (countProcessedInstanceEdgesLock) {
              countProcessedInstanceEdges++;
            }
          }
          final ByteBuffer allocate;
          final EdgeFBDataMinimized edata = vertex.outEdge(i).getValue();
          if (context.getIteration() == 0) {
            allocate = ByteBuffer.allocate(2);
            allocate.putShort((short) (edata.predicateid + 1));
          } else {
            final byte[] bb = getPreviousBlockId(outEdgeVertexId);

            if (flagTrackStability && flagNodeStable && context.getIteration() > 1) { // fw stability check
              if (MDB_KMinusOnePartitionBlockSizes.get(bb).intValue() != MDB_KMinusTwoPartitionBlockSizes.get(previousPartitionKMinusTwo.get(outEdgeVertexId)).intValue()) {
                flagNodeStable = false;
              }
            }
            allocate = ByteBuffer.allocate(bb.length + 2);
            allocate.putShort((short) (edata.predicateid + 1));
            allocate.put(bb);
          }
          allocate.rewind();
          hs.add(allocate);
        }
      }

      if (dir_B) {
        // bw
        for (int i = 0; i < vertex.numInEdges(); i++) {
          final int inEdgeVertexId = vertex.inEdge(i).getVertexId();
          if (bs_allDisabled.get(inEdgeVertexId)) {
            continue;
          }
          if (flagCountProcessedInstanceEdges) {
            synchronized (countProcessedInstanceEdgesLock) {
              countProcessedInstanceEdges++;
            }
          }
          final ByteBuffer allocate;
          final EdgeFBDataMinimized edata = vertex.inEdge(i).getValue();
          if (context.getIteration() == 0) {
            allocate = ByteBuffer.allocate(2);
            allocate.putShort((short) ((-edata.predicateid) - 1));
          } else {
            final byte[] bb = getPreviousBlockId(inEdgeVertexId);
            if (flagTrackStability && flagNodeStable && context.getIteration() > 1) { // fw stability check
              if (MDB_KMinusOnePartitionBlockSizes.get(bb).intValue() != MDB_KMinusTwoPartitionBlockSizes.get(previousPartitionKMinusTwo.get(inEdgeVertexId)).intValue()) {
                flagNodeStable = false;
              }
            }
            // turn negative into a boolean or complement of bb
            allocate = ByteBuffer.allocate(bb.length + 2);
            allocate.putShort((short) ((-edata.predicateid) - 1));
            allocate.put(bb);
          }
          allocate.rewind();
          hs.add(allocate);
        }
      }

      if (flagTrackStability && flagNodeStable && context.getIteration() > 1) {
        synchronized (numstableLock) {
          numstable++;
        }

        if (MDB_KMinusOnePartitionBlockSizes.get(bhere) == 1) {
          synchronized (numstablesingletonLock) {
            numstablesingleton++;
          }
        }

        synchronized (stableBlocks) {
          stableBlocks.add(bhere);
        }

        if (flagSkipStable) {
          setNodeBlockId(vertexid, bhere);
          incrementExtentSize(bhere);
          // skip stable nodes
          return;
        }
      }

      computeNodeSignature(hs, bhere, md);

      // exec
      final byte[] newh = md.digest();

      // set node's new block id
      setNodeBlockId(vertexid, newh);

      if (flagCountBlockSizes || flagSkipSingletons || flagCountSingletons) {
        incrementExtentSize(newh);
      } else {
        MDB_currentPartitionBlockSizes.putIfAbsent(newh, 1);
      }

      trackRefinementSet(context, bhere, newh);

    } catch (NoSuchAlgorithmException ex) {
    }

  }

  /**
   * Method to return vertexid's block id from the last, completed, iteration
   *
   * @param vertexid vertex for which to get block id
   * @return byte-based block id hash value, zero if it does not exist
   */
  public byte[] getPreviousBlockId(final int vertexid) {
    byte[] bhere;
    bhere = previousPartitionKMinusOne.get(vertexid);
    if (bhere == null) {
      bhere = new byte[1];
      bhere[0] = 0;
    }
    return bhere;
  }

  public boolean skipStable(final GraphChiContext context, byte[] bhere, final int vertexid) {
    // skip nodes that are in a stable block already
    if (flagTrackStability && flagSkipStable && context.getIteration() > 1) {
      boolean flagT = false;
      synchronized (stableBlocks) {
        if (stableBlocks.contains(bhere)) {
          flagT = true;
        }
      }
      if (flagT && flagSkipStable) {
        synchronized (numstableLock) {
          numstable++;
        }
        setNodeBlockId(vertexid, bhere);
        incrementExtentSize(bhere);
        // skip stable nodes
        return true;
      }
    }
    return false;
  }

  public void trackRefinementSet(final GraphChiContext context, byte[] bhere, final byte[] newh) {
    if (!flagCountEdgesOnly && flagDoPrint && context.getIteration() > 0 && flagTrackRefinementSet) {
      synchronized (refinementSet) {
        refinementSet.add(DigestUtils.md5Hex(bhere) + " " + DigestUtils.md5Hex(newh));
      }
    }
  }

  final static ByteComparatorUpdate bytecomparator = new ByteComparatorUpdate();

  public void computeNodeSignature(final Set<ByteBuffer> hs, byte[] bhere, final MessageDigest md) {
    final Set<byte[]> ts = new TreeSet<>(bytecomparator);

    hs.stream().forEach((bb) -> {
      ts.add(bb.array());
    });

    // record previous blockid as part of signature
    // since new block id should refine previous block
    if (bhere != null) {
      md.update(bhere);
    }

    ts.stream().forEach((bb) -> {
      md.update(bb);
    });
  }

  /**
   * Set the current iteration's block id of the vertex
   *
   * @param vertexid vertex to set the block id for
   * @param bhere byte-based hash value of vertex block id
   */
  public void setNodeBlockId(final int vertexid, byte[] bhere) {
    // set the vertex's block id
    currentPartition.put(vertexid, bhere);
  }

  /**
   *
   * @param bhere
   */
  public void incrementExtentSize(byte[] bhere) {
    // update the block's extent size
    synchronized (MDB_currentPartitionBlockSizes) {
      final Integer bsz = MDB_currentPartitionBlockSizes.putIfAbsent(bhere, 1);
      if (bsz != null) {
        MDB_currentPartitionBlockSizes.put(bhere, (bsz + 1));
      }
    }
  }

  public void printSummaryEdges(final GraphChiContext context, final ChiVertex<NodeFBDataMinimized, EdgeFBDataMinimized> vertex, byte[] bhere) {
    HashSet<Triplet<ByteBuffer, Integer, ByteBuffer>> localHashes = new HashSet<>();

    for (int i = 0; i < vertex.numOutEdges(); i++) {
      final ChiEdge<EdgeFBDataMinimized> outEdge = vertex.outEdge(i);

      final int vid = vertex.getId();
      final int tovertexid = vertex.getOutEdgeId(i);
      final byte[] toblock = getPreviousBlockId(tovertexid);

      final int p = outEdge.getValue().predicateid;

      final Triplet<ByteBuffer, Integer, ByteBuffer> triplet = new Triplet<>(ByteBuffer.wrap(bhere), p, ByteBuffer.wrap(toblock));
      if (!localHashes.contains(triplet)) {
        localHashes.add(triplet);
      } else {
        continue;
      }

      if (flagCountEdgesOnly) {
        synchronized (summaryCountedEdgesLock) {
          summaryCountedEdges++;
        }
        continue;
      }

      final String s, o;

      if (flagSkipSingletons && flagPrintMDL && bs_allDisabled.get(vertex.getId())) {
        final int backwardId = context.getVertexIdTranslate().backward(vid);
        s = Fun.filter(inv_nodes, backwardId).iterator().next();
      } else {
        s = "<http://bc.org/" + DigestUtils.md5Hex(bhere) + ">";
      }
      if (flagSkipSingletons && flagPrintMDL && bs_allDisabled.get(tovertexid)) {
        final int backwardId = context.getVertexIdTranslate().backward(tovertexid);
        o = Fun.filter(inv_nodes, backwardId).iterator().next();
      } else {
        o = "<http://bc.org/" + DigestUtils.md5Hex(toblock) + ">";
      }

      final String predString = inv_preds2.get(p);
      final String line = s + " " + predString + " " + o + " .";
      summaryEdgesWriter.println(line);
    }
  }

  @Override
  public void beginIteration(final GraphChiContext ctx) {
    // just setup timer for start of iteration
    itertimer = new Timer("itertimer-" + ctx.getIteration(), true);
  }

  /**
   * Method executed by GraphChi at the end of each iteration
   *
   * @param ctx
   */
  @Override
  public void endIteration(final GraphChiContext ctx) {

    // no operations needed if just printing
    if (endIterationDoPrint(ctx)) {
      return;
    }

    simplelogger.log(itertimer.stop() + "");

    int numStableBlocks = stableBlocks.size();

    simplelogger.log("numvertices:" + numvertices);
    simplelogger.log("blocksizes:" + MDB_KMinusOnePartitionBlockSizes.size());
    simplelogger.log("blocksizessnext:" + MDB_currentPartitionBlockSizes.size());
    simplelogger.log("numSingleton:" + singletonCount);
    simplelogger.log("summaryCountedEdges:" + summaryCountedEdges);
    simplelogger.log("numStableNodes:" + numstable);
    simplelogger.log("numStableSingletons:" + numstablesingleton);
    simplelogger.log("numStableBlocks:" + stableBlocks.size());

    simplelogger.log("countProcessedInstanceEdges:" + countProcessedInstanceEdges);
    countProcessedInstanceEdges = 0;

    // TODO: improve the way counting is done
    int cs;
    cs = MDB_currentPartitionBlockSizes.size();
    if (flagSkipSingletons) {
      cs += singletonCount;
    }

    simplelogger.log("counts:::" + cs);

    // obtain the reference to the history object
    // TODO: don't use static access
    Stack<HistoryItemVerB> countHistory = GraphChiVSMainVerExperimentNDMinimized.countHistory;

    final HistoryItemVerB hi = new HistoryItemVerB();
    hi.iteration = ctx.getIteration();
    hi.value = cs;
    hi.dir_F = dir_F;
    hi.dir_B = dir_B;
    hi.time_s = itertimer.getTimeS();
    hi.singletonNodeCount = singletonCount;
    hi.stableNodeCount = numstable;
    hi.stableBlockCount = numStableBlocks;
    hi.stableSingletonCount = numstablesingleton;
    countHistory.push(hi);

    simplelogger.log("iteration:" + ctx.getIteration());
    simplelogger.log("blocks:" + hi.value);
    simplelogger.log("history:\n" + HistoryItemVerB.schema() + "\n" + countHistory.toString().replace(">,", "\n").replace("<", "").replace("[", "").replace("]", "").replace(">", "").replace(" ", "").trim());

    // validation check
    if (countHistory.size() > 1 && countHistory.elementAt(countHistory.size() - 1).value < countHistory.elementAt(countHistory.size() - 2).value) {
      System.err.println("Incorrect partition discovered");
      System.exit(1);
    } else if (countHistory.size() > 1 && countHistory.elementAt(countHistory.size() - 1).value == countHistory.elementAt(countHistory.size() - 2).value) {
      // we are done, so empty scheduler of pending tasks
      ctx.getScheduler().removeAllTasks();
      if (flagDoPrint) {

        // write summary extents to disk
        doPrintStuff(ctx);

        // need to execute one more iteration in order to print summary edges
        flagLastIteration = true;
        ctx.getScheduler().addAllTasks();
      }

      return;
    }

    System.out.println("bs_allDisabled1:" + bs_allDisabled.cardinality());

    // update bitsets keeping track of previous-to-current disabled and disabled in current iteration 
    bs_allDisabled.or(bs_toDisable);
    bs_toDisable.clear();

    System.out.println("bs_allDisabled2:" + bs_allDisabled.cardinality());

    ctx.getScheduler().addAllTasks();
    for (int i = bs_allDisabled.nextSetBit(0); i >= 0; i = bs_allDisabled.nextSetBit(i + 1)) {
      ctx.getScheduler().removeTasks(i, i);
    }

    if (flagTrackStability) {
      endIterationStabilityVariation(ctx);

    } else {
      final Map<Integer, byte[]> tempPartition = previousPartitionKMinusOne;
      previousPartitionKMinusOne = currentPartition;
      currentPartition = tempPartition;

      DBmaker.delete(DBmaker.getNameForObject(MDB_KMinusOnePartitionBlockSizes));
      MDB_KMinusOnePartitionBlockSizes = MDB_currentPartitionBlockSizes;
      MDB_currentPartitionBlockSizes = DBmaker.createHashMap("blocksizesnext" + ctx.getIteration()).hasher(Hasher.BYTE_ARRAY).keySerializer(Serializer.BYTE_ARRAY).make();
    }

    System.out.println("bs_allDisabled:" + bs_allDisabled.cardinality());

    if (flagCountSingletons && !flagSkipSingletons) {
      singletonCount = 0;
    }
    numstable = 0;
    numstablesingleton = 0;
    stableBlocks.clear();
  }

  /**
   * Method to close summary writer at the end of printing and to output
   * refinements TODO: cleanup this method, move refinements writing to another
   * method which can be executed at the end of each iteration
   *
   * @param ctx context that contains GraphChi's current iteration
   * @return
   */
  public boolean endIterationDoPrint(final GraphChiContext ctx) {
    try {
      if (flagDoPrint) {
        if (flagLastIteration) {
          summaryEdgesWriter.flush();
          summaryEdgesWriter.close();
          ctx.getScheduler().removeAllTasks();
          return true;
        }
        if (!flagCountEdgesOnly) {
          final PrintWriter pwRefinements = FileUtil.getPrintWriter("refinements-" + ctx.getIteration() + ".txt.gz");
          refinementSet.stream().forEach((ab) -> {
            final String parse[] = ab.split(" ");
            pwRefinements.println("<http://bc.org/" + parse[0] + "> <http://bc.org/refinesTo> <http://bc.org/" + parse[1] + "> .");

          });

          pwRefinements.flush();
          pwRefinements.close();
          refinementSet.clear();
        }
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }
    return false;
  }

  /**
   * Method that writes last partition's summary extents to disk TODO: cleanup
   * and potentially refactor class
   *
   * @param ctx context which contains mapping from node ids to
   * GraphChi-internal ids
   */
  public void doPrintStuff(final GraphChiContext ctx) {
    if (!flagCountEdgesOnly) {
      try {
        PrintWriter pwbpi = FileUtil.getPrintWriter("graphchivs-lastpartition-bpi.txt.gz");
        for (Integer vid : previousPartitionKMinusOne.keySet()) {
          try {
            if (vid == null) {
              System.err.println("*** ERROR, NODE WITH NULL ID EXISTS IN PREVIOUSPARTITION.");
            }
            int backwardId = ctx.getVertexIdTranslate().backward(vid);
            final String nodeString = Fun.filter(inv_nodes, backwardId).iterator().next();

            byte[] prevBlock = getPreviousBlockId(vid);
            if (flagPrintMDL && !MDB_KMinusOnePartitionBlockSizes.containsKey(prevBlock)) {
              // skip printing extent of singleton blocks
              continue;
            }
            final String prevPartitionString = DigestUtils.md5Hex(prevBlock);
            pwbpi.write("<http://bc.org/" + prevPartitionString + "> <http://bc.org/hasExtent> " + nodeString + " .\n");
          } catch (Exception e) {
            e.printStackTrace();
            System.err.println("***Node with id not found, cannot print block for vid:" + vid);
          }
        }
        pwbpi.flush();
        pwbpi.close();

      } catch (IOException ex) {
        System.err.println("*** EXCEPTION ***");
      }
    }
    return;
  }

  /**
   * Cleanup data structures when tracking stability (involves k-2 iteration)
   *
   * @param ctx context which contains GraphChi's current iteration number
   */
  public void endIterationStabilityVariation(final GraphChiContext ctx) {
    if (previousPartitionKMinusTwo != null) {
      try {
        DBmaker.delete(DBmaker.getNameForObject(previousPartitionKMinusTwo));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    previousPartitionKMinusTwo = previousPartitionKMinusOne;
    previousPartitionKMinusOne = currentPartition;
    currentPartition = DBmaker.createHashMap("currentpartition-V1-" + ctx.getIteration()).valueSerializer(Serializer.BYTE_ARRAY).make();

    if (MDB_KMinusTwoPartitionBlockSizes != null) {
      try {
        DBmaker.delete(DBmaker.getNameForObject(MDB_KMinusTwoPartitionBlockSizes));
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    MDB_KMinusTwoPartitionBlockSizes = MDB_KMinusOnePartitionBlockSizes;
    MDB_KMinusOnePartitionBlockSizes = MDB_currentPartitionBlockSizes;
    MDB_currentPartitionBlockSizes = DBmaker.createHashMap("blocksizesnextEI" + ctx.getIteration()).hasher(Hasher.BYTE_ARRAY).keySerializer(Serializer.BYTE_ARRAY).make();
  }

  @Override
  public void beginInterval(GraphChiContext ctx, VertexInterval interval
  ) {
    // nop
  }

  @Override
  public void endInterval(GraphChiContext ctx, VertexInterval interval
  ) {
    // nop
  }

  @Override
  public void beginSubInterval(GraphChiContext ctx, VertexInterval interval
  ) {
    // nop
  }

  @Override
  public void endSubInterval(GraphChiContext ctx, VertexInterval interval
  ) {
    // nop
  }

}
