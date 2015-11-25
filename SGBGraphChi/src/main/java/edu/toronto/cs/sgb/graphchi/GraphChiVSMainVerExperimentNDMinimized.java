package edu.toronto.cs.sgb.graphchi;

import edu.cmu.graphchi.GraphChiProgram;
import edu.cmu.graphchi.engine.GraphChiEngine;
import edu.cmu.graphchi.preprocessing.EdgeProcessor;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexProcessor;
import edu.toronto.cs.sgb.util.FileUtil;
import edu.toronto.cs.sgb.util.SimpleLogger;
import edu.toronto.cs.sgb.util.Timer;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Stack;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

/**
 * Non-deterministic singleton-optimization bisimulation computation using
 * GraphChi
 *
 */
public class GraphChiVSMainVerExperimentNDMinimized {

  public static String graphName;

  // limit to number of lines to injest
  public static int linelimit = Integer.MAX_VALUE;
//	public static int linelimit = 100;

  // debug flags
  static boolean flagShowInstances = false;
  static boolean flagShowBlocks = false;

  // flag to filter predicates (for selective summaries)
  final static boolean flagFilterPreds = false;

  // flag to indicate whether input is integer, or string that is mapped to integer by this app
  final static boolean flagIntIn = false; // incoming statements are triples of int ids

  // keep track of partition statistics for each iteration
  public static Stack<HistoryItemVerB> countHistory = new Stack<HistoryItemVerB>();

  // limit number of iterations
  private static final int niters = Integer.MAX_VALUE;

  // default number of shards
  private static int shards = 3;

  // in-memory off-heap map
  private static DB DBmaker;

  // map from predicate strings to ints, and backwards as well for inverse
  private final HashMap<String, Integer> predIds2;
  private final HTreeMap<String, Integer> predIds;
  private int predid_currentindex = 1;

  // map from node strings to ints
  private final HTreeMap<String, Integer> nodeIds;
  private int nodeid_currentindex;

  public GraphChiVSMainVerExperimentNDMinimized() {
    DBMaker temp = DBMaker.newMemoryDirectDB();
    temp = temp.transactionDisable().deleteFilesAfterClose().closeOnJvmShutdown();
    temp = temp.asyncWriteEnable();
    temp = temp.cacheSize(1000000);
    temp = temp.freeSpaceReclaimQ(5);
    DBmaker = temp.make();

    nodeIds = DBmaker.createHashMap("nodes").make();
    predIds = DBmaker.createHashMap("preds").make();

    predIds2 = new HashMap<>();

    if (flagFilterPreds) {
      addPredConstraints();
    }
  }

  private void run(String[] args) {

    if (args.length != 3) {
      System.out.println("Usage: <input> <numshards> <S|B,F|B|(D)ual>, e.g. SF, BF, SB, BB, SD, BD");
      System.exit(1);
    }

    if (args.length == 3 && args[2].length() == 2) {
      if (args[2].charAt(0) == 'B') {
        GraphChivsProgramVerExperimentNDMinimized.flagSkipSingletons = false;
      } else if (args[2].charAt(0) == 'S') {
        GraphChivsProgramVerExperimentNDMinimized.flagSkipSingletons = true;
      }
      if (args[2].charAt(1) == 'B') {
        GraphChivsProgramVerExperimentNDMinimized.dir_F = false;
        GraphChivsProgramVerExperimentNDMinimized.dir_B = true;
      } else if (args[2].charAt(1) == 'D') {
        GraphChivsProgramVerExperimentNDMinimized.dir_F = true;
        GraphChivsProgramVerExperimentNDMinimized.dir_B = true;
      } else if (args[2].charAt(1) == 'F') {
        GraphChivsProgramVerExperimentNDMinimized.dir_F = true;
        GraphChivsProgramVerExperimentNDMinimized.dir_B = false;
      }
    } else {
      // default to SD
      GraphChivsProgramVerExperimentNDMinimized.flagSkipSingletons = true;
      GraphChivsProgramVerExperimentNDMinimized.dir_F = true;
      GraphChivsProgramVerExperimentNDMinimized.dir_B = true;

    }

    simplelogger = new SimpleLogger();
    try {
      simplelogger.setup(this.getClass().getCanonicalName() + "-logfile");
    } catch (IOException ex) {

    }
    simplelogger.log("Starting Program Ver Experiment ND (Non-deterministic setting) with minimization:" + this.getClass().getCanonicalName());

    graphName = args[0];
    shards = Integer.parseInt(args[1]);
    simplelogger.log("config used:" + args[0] + ":" + args[1] + ":" + ((args.length > 2) ? args[2] : ""));

    boolean flagTest = false;

    if (!flagTest) {
      Reader is = null;
//			if (!new File(ChiFilenames.getFilenameIntervals(graphName, shards)).exists()) {
      simplelogger.log("Starting to load new shard data from file");
      try {
        is = FileUtil.getBufferedReader(graphName);
      } catch (FileNotFoundException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }

      preprocess(graphName, shards, is);
    } else {
      // testing portion
      simplelogger.log("Manual entry!!!!!");
      String s = "0	1\n";
      
      final FastSharder<NodeFBDataMinimized, EdgeFBDataMinimized> sharder;
      try {
        sharder = createSharder(graphName, shards);

        sharder.process();
      } catch (IOException e) {
        e.printStackTrace();
      }

    }

    try {
      // initialize GraphChi engine 
      final GraphChiEngine<NodeFBDataMinimized, EdgeFBDataMinimized> engine = new GraphChiEngine<NodeFBDataMinimized, EdgeFBDataMinimized>(graphName, shards);
      engine.setVertexDataConverter(new NodeFBDataMinimizedConverter());
      engine.setEdataConverter(new EdgeFBDataMinimizedConverter());
      engine.setSkipZeroDegreeVertices(true);
      engine.setEnableScheduler(true);
      engine.setEnableDeterministicExecution(false);
      engine.setModifiesInedges(false);
      engine.setModifiesOutedges(false);

      // create program
      final GraphChiProgram<NodeFBDataMinimized, EdgeFBDataMinimized> program = new GraphChivsProgramVerExperimentNDMinimized(DBmaker, engine, simplelogger, nodeIds, predIds2);

      simplelogger.log("Number of vertices engine:" + engine.numVertices());

      // run the program
      final Timer t = new Timer("program", true);
      simplelogger.log(t.start());
      engine.run(program, Integer.MAX_VALUE);
      simplelogger.log(t.stop().toString());

      simplelogger.log("countHistory:" + countHistory);
      simplelogger.log("summaryCountedEdges:" + ((GraphChivsProgramVerExperimentNDMinimized) program).summaryCountedEdges);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  ArrayList<String> predConstraints = new ArrayList<>();
  SimpleLogger simplelogger;

  public enum PredConstraint {

    D13, D30, D600, FULL
  }

  public void addPredConstraints() {
    // p3
//		predConstraints.add("<http://dbpedia.org/ontology/country>");
//		predConstraints.add("<http://dbpedia.org/ontology/isPartOf>");
//		predConstraints.add("<http://purl.org/dc/terms/subject>");

    PredConstraint pc = PredConstraint.D13;

    // P30Q
    if (pc.equals(PredConstraint.D30)) {
      predConstraints.add("<http://dbpedia.org/ontology/binomialAuthority>");
      predConstraints.add("<http://dbpedia.org/ontology/careerStation>"); // does not occur in log (though appears in old query)
      predConstraints.add("<http://dbpedia.org/ontology/country>");
      predConstraints.add("<http://dbpedia.org/ontology/department>");
      predConstraints.add("<http://dbpedia.org/ontology/district>");
      predConstraints.add("<http://dbpedia.org/ontology/division>");
      predConstraints.add("<http://dbpedia.org/ontology/family>");
      predConstraints.add("<http://dbpedia.org/ontology/genus>");
      predConstraints.add("<http://dbpedia.org/ontology/hybrid>");  // does not occur in log (though appears in old query)
      predConstraints.add("<http://dbpedia.org/ontology/isPartOf>");
      predConstraints.add("<http://dbpedia.org/ontology/kingdom>");
      predConstraints.add("<http://dbpedia.org/ontology/leaderName>");
      predConstraints.add("<http://dbpedia.org/ontology/neighboringMunicipality>");
      predConstraints.add("<http://dbpedia.org/ontology/parent>");
      predConstraints.add("<http://dbpedia.org/ontology/phylum>");
      predConstraints.add("<http://dbpedia.org/ontology/spokenIn>");
      predConstraints.add("<http://dbpedia.org/ontology/team>");
      predConstraints.add("<http://dbpedia.org/ontology/timeZone>");
      predConstraints.add("<http://dbpedia.org/ontology/type>");
      predConstraints.add("<http://dbpedia.org/ontology/wikiPageExternalLink>");
      predConstraints.add("<http://dbpedia.org/ontology/wikiPageRedirects>");
      predConstraints.add("<http://dbpedia.org/ontology/wikiPageWikiLink>");
      predConstraints.add("<http://dbpedia.org/property/hasPhotoCollection>");
      predConstraints.add("<http://dbpedia.org/property/wordnet_type>");
      predConstraints.add("<http://purl.org/dc/terms/subject>");
      predConstraints.add("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
      predConstraints.add("<http://www.w3.org/2002/07/owl#sameAs>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/homepage>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/isPrimaryTopicOf>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/primaryTopic>");
    } else if (pc.equals(PredConstraint.D600)) {
      // 600 preds
      predConstraints.add("<http://dbpedia.org/ontology/wikiPageWikiLink>");
      predConstraints.add("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>");
      predConstraints.add("<http://purl.org/dc/terms/subject>");
      predConstraints.add("<http://www.w3.org/2000/01/rdf-schema#label>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/isPrimaryTopicOf>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/primaryTopic>");
      predConstraints.add("<http://purl.org/dc/elements/1.1/language>");
      predConstraints.add("<http://dbpedia.org/ontology/wikiPageExternalLink>");
      predConstraints.add("<http://dbpedia.org/ontology/wikiPageRedirects>");
      predConstraints.add("<http://www.w3.org/2002/07/owl#sameAs>");
      predConstraints.add("<http://dbpedia.org/property/hasPhotoCollection>");
      predConstraints.add("<http://purl.org/dc/elements/1.1/rights>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/name>");
      predConstraints.add("<http://www.w3.org/2004/02/skos/core#broader>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/depiction>");
      predConstraints.add("<http://dbpedia.org/ontology/thumbnail>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/thumbnail>");
      predConstraints.add("<http://dbpedia.org/ontology/team>");
      predConstraints.add("<http://dbpedia.org/ontology/wikiPageDisambiguates>");
      predConstraints.add("<http://www.w3.org/2004/02/skos/core#prefLabel>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/givenName>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/surname>");
      predConstraints.add("<http://dbpedia.org/ontology/birthPlace>");
      predConstraints.add("<http://www.w3.org/2003/01/geo/wgs84_pos#lat>");
      predConstraints.add("<http://www.w3.org/2003/01/geo/wgs84_pos#long>");
      predConstraints.add("<http://www.georss.org/georss/point>");
      predConstraints.add("<http://purl.org/dc/elements/1.1/description>");
      predConstraints.add("<http://dbpedia.org/ontology/birthDate>");
      predConstraints.add("<http://dbpedia.org/ontology/isPartOf>");
      predConstraints.add("<http://dbpedia.org/ontology/country>");
      predConstraints.add("<http://dbpedia.org/ontology/careerStation>");
      predConstraints.add("<http://dbpedia.org/ontology/years>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/homepage>");
      predConstraints.add("<http://dbpedia.org/ontology/genre>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfGoals>");
      predConstraints.add("<http://www.w3.org/2002/07/owl#equivalentClass>");
      predConstraints.add("<http://www.w3.org/2000/01/rdf-schema#subClassOf>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfMatches>");
      predConstraints.add("<http://dbpedia.org/property/wordnet_type>");
      predConstraints.add("<http://dbpedia.org/ontology/position>");
      predConstraints.add("<http://dbpedia.org/ontology/type>");
      predConstraints.add("<http://dbpedia.org/ontology/family>");
      predConstraints.add("<http://dbpedia.org/ontology/location>");
      predConstraints.add("<http://dbpedia.org/ontology/starring>");
      predConstraints.add("<http://dbpedia.org/ontology/deathDate>");
      predConstraints.add("<http://dbpedia.org/ontology/populationTotal>");
      predConstraints.add("<http://dbpedia.org/ontology/occupation>");
      predConstraints.add("<http://dbpedia.org/ontology/order>");
      predConstraints.add("<http://dbpedia.org/ontology/class>");
      predConstraints.add("<http://dbpedia.org/ontology/currentMember>");
      predConstraints.add("<http://dbpedia.org/ontology/deathPlace>");
      predConstraints.add("<http://dbpedia.org/ontology/recordLabel>");
      predConstraints.add("<http://dbpedia.org/ontology/kingdom>");
      predConstraints.add("<http://dbpedia.org/ontology/producer>");
      predConstraints.add("<http://dbpedia.org/ontology/utcOffset>");
      predConstraints.add("<http://dbpedia.org/ontology/timeZone>");
      predConstraints.add("<http://dbpedia.org/ontology/runtime>");
      predConstraints.add("<http://dbpedia.org/ontology/phylum>");
      predConstraints.add("<http://dbpedia.org/ontology/elevation>");
      predConstraints.add("<http://dbpedia.org/ontology/squadNumber>");
      predConstraints.add("<http://dbpedia.org/ontology/postalCode>");
      predConstraints.add("<http://dbpedia.org/ontology/releaseDate>");
      predConstraints.add("<http://dbpedia.org/ontology/genus>");
      predConstraints.add("<http://dbpedia.org/ontology/activeYearsStartYear>");
      predConstraints.add("<http://dbpedia.org/ontology/writer>");
      predConstraints.add("<http://dbpedia.org/ontology/subsequentWork>");
      predConstraints.add("<http://dbpedia.org/ontology/areaTotal>");
      predConstraints.add("<http://dbpedia.org/ontology/previousWork>");
      predConstraints.add("<http://dbpedia.org/ontology/title>");
      predConstraints.add("<http://dbpedia.org/ontology/artist>");
      predConstraints.add("<http://dbpedia.org/ontology/associatedBand>");
      predConstraints.add("<http://dbpedia.org/ontology/associatedMusicalArtist>");
      predConstraints.add("<http://dbpedia.org/ontology/areaCode>");
      predConstraints.add("<http://dbpedia.org/ontology/formerTeam>");
      predConstraints.add("<http://dbpedia.org/ontology/birthYear>");
      predConstraints.add("<http://dbpedia.org/ontology/city>");
      predConstraints.add("<http://dbpedia.org/ontology/hometown>");
      predConstraints.add("<http://dbpedia.org/ontology/director>");
      predConstraints.add("<http://dbpedia.org/ontology/language>");
      predConstraints.add("<http://dbpedia.org/ontology/height>");
      predConstraints.add("<http://dbpedia.org/ontology/activeYearsStartDate>");
      predConstraints.add("<http://dbpedia.org/ontology/battle>");
      predConstraints.add("<http://dbpedia.org/ontology/nationality>");
      predConstraints.add("<http://dbpedia.org/ontology/format>");
      predConstraints.add("<http://dbpedia.org/ontology/activeYearsEndDate>");
      predConstraints.add("<http://dbpedia.org/ontology/foundingYear>");
      predConstraints.add("<http://dbpedia.org/ontology/successor>");
      predConstraints.add("<http://dbpedia.org/ontology/binomialAuthority>");
      predConstraints.add("<http://dbpedia.org/ontology/activeYearsEndYear>");
      predConstraints.add("<http://dbpedia.org/ontology/region>");
      predConstraints.add("<http://dbpedia.org/ontology/background>");
      predConstraints.add("<http://dbpedia.org/ontology/almaMater>");
      predConstraints.add("<http://dbpedia.org/ontology/synonym>");
      predConstraints.add("<http://dbpedia.org/ontology/award>");
      predConstraints.add("<http://dbpedia.org/ontology/termPeriod>");
      predConstraints.add("<http://dbpedia.org/ontology/length>");
      predConstraints.add("<http://dbpedia.org/ontology/publisher>");
      predConstraints.add("<http://dbpedia.org/ontology/office>");
      predConstraints.add("<http://dbpedia.org/ontology/weight>");
      predConstraints.add("<http://dbpedia.org/ontology/instrument>");
      predConstraints.add("<http://dbpedia.org/ontology/division>");
      predConstraints.add("<http://dbpedia.org/ontology/area>");
      predConstraints.add("<http://dbpedia.org/ontology/district>");
      predConstraints.add("<http://dbpedia.org/ontology/party>");
      predConstraints.add("<http://dbpedia.org/ontology/populationDensity>");
      predConstraints.add("<http://dbpedia.org/ontology/number>");
      predConstraints.add("<http://dbpedia.org/ontology/recordedIn>");
      predConstraints.add("<http://dbpedia.org/ontology/conservationStatus>");
      predConstraints.add("<http://dbpedia.org/ontology/conservationStatusSystem>");
      predConstraints.add("<http://dbpedia.org/ontology/distributor>");
      predConstraints.add("<http://dbpedia.org/ontology/musicalArtist>");
      predConstraints.add("<http://dbpedia.org/ontology/musicalBand>");
      predConstraints.add("<http://dbpedia.org/ontology/author>");
      predConstraints.add("<http://dbpedia.org/ontology/birthName>");
      predConstraints.add("<http://dbpedia.org/ontology/musicComposer>");
      predConstraints.add("<http://dbpedia.org/ontology/owner>");
      predConstraints.add("<http://dbpedia.org/ontology/computingPlatform>");
      predConstraints.add("<http://dbpedia.org/ontology/areaLand>");
      predConstraints.add("<http://dbpedia.org/ontology/residence>");
      predConstraints.add("<http://dbpedia.org/ontology/managerClub>");
      predConstraints.add("<http://dbpedia.org/ontology/deathYear>");
      predConstraints.add("<http://dbpedia.org/ontology/state>");
      predConstraints.add("<http://dbpedia.org/ontology/areaWater>");
      predConstraints.add("<http://dbpedia.org/ontology/nrhpReferenceNumber>");
      predConstraints.add("<http://dbpedia.org/ontology/added>");
      predConstraints.add("<http://dbpedia.org/ontology/maximumElevation>");
      predConstraints.add("<http://dbpedia.org/ontology/product>");
      predConstraints.add("<http://dbpedia.org/ontology/status>");
      predConstraints.add("<http://dbpedia.org/ontology/minimumElevation>");
      predConstraints.add("<http://dbpedia.org/ontology/industry>");
      predConstraints.add("<http://dbpedia.org/ontology/inseeCode>");
      predConstraints.add("<http://dbpedia.org/ontology/department>");
      predConstraints.add("<http://dbpedia.org/ontology/alias>");
      predConstraints.add("<http://dbpedia.org/ontology/commander>");
      predConstraints.add("<http://dbpedia.org/ontology/leaderTitle>");
      predConstraints.add("<http://xmlns.com/foaf/0.1/nick>");
      predConstraints.add("<http://dbpedia.org/ontology/yearOfConstruction>");
      predConstraints.add("<http://dbpedia.org/ontology/league>");
      predConstraints.add("<http://dbpedia.org/ontology/militaryBranch>");
      predConstraints.add("<http://dbpedia.org/ontology/builder>");
      predConstraints.add("<http://www.w3.org/2004/02/skos/core#related>");
      predConstraints.add("<http://dbpedia.org/ontology/locatedInArea>");
      predConstraints.add("<http://dbpedia.org/ontology/ground>");
      predConstraints.add("<http://dbpedia.org/ontology/headquarter>");
      predConstraints.add("<http://dbpedia.org/ontology/guest>");
      predConstraints.add("<http://dbpedia.org/ontology/sisterStation>");
      predConstraints.add("<http://dbpedia.org/ontology/cinematography>");
      predConstraints.add("<http://dbpedia.org/ontology/field>");
      predConstraints.add("<http://dbpedia.org/ontology/knownFor>");
      predConstraints.add("<http://dbpedia.org/ontology/motto>");
      predConstraints.add("<http://dbpedia.org/ontology/keyPerson>");
      predConstraints.add("<http://dbpedia.org/ontology/religion>");
      predConstraints.add("<http://dbpedia.org/ontology/bandMember>");
      predConstraints.add("<http://dbpedia.org/ontology/literaryGenre>");
      predConstraints.add("<http://dbpedia.org/ontology/network>");
      predConstraints.add("<http://dbpedia.org/ontology/race>");
      predConstraints.add("<http://dbpedia.org/ontology/routeJunction>");
      predConstraints.add("<http://dbpedia.org/ontology/orderInOffice>");
      predConstraints.add("<http://dbpedia.org/ontology/developer>");
      predConstraints.add("<http://dbpedia.org/ontology/openingYear>");
      predConstraints.add("<http://dbpedia.org/ontology/address>");
      predConstraints.add("<http://dbpedia.org/ontology/spouse>");
      predConstraints.add("<http://dbpedia.org/ontology/county>");
      predConstraints.add("<http://dbpedia.org/ontology/creator>");
      predConstraints.add("<http://dbpedia.org/ontology/parent>");
      predConstraints.add("<http://dbpedia.org/ontology/stateOfOrigin>");
      predConstraints.add("<http://dbpedia.org/ontology/shipBeam>");
      predConstraints.add("<http://dbpedia.org/ontology/formerBandMember>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfStudents>");
      predConstraints.add("<http://dbpedia.org/ontology/isbn>");
      predConstraints.add("<http://dbpedia.org/ontology/series>");
      predConstraints.add("<http://dbpedia.org/ontology/influencedBy>");
      predConstraints.add("<http://dbpedia.org/ontology/editing>");
      predConstraints.add("<http://dbpedia.org/ontology/architecturalStyle>");
      predConstraints.add("<http://dbpedia.org/ontology/place>");
      predConstraints.add("<http://dbpedia.org/ontology/origin>");
      predConstraints.add("<http://dbpedia.org/ontology/gridReference>");
      predConstraints.add("<http://dbpedia.org/ontology/languageFamily>");
      predConstraints.add("<http://dbpedia.org/ontology/routeStart>");
      predConstraints.add("<http://dbpedia.org/ontology/mediaType>");
      predConstraints.add("<http://dbpedia.org/ontology/locationCity>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfPages>");
      predConstraints.add("<http://dbpedia.org/ontology/coachedTeam>");
      predConstraints.add("<http://dbpedia.org/ontology/programmeFormat>");
      predConstraints.add("<http://dbpedia.org/ontology/broadcastArea>");
      predConstraints.add("<http://dbpedia.org/ontology/routeEnd>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfEpisodes>");
      predConstraints.add("<http://dbpedia.org/ontology/album>");
      predConstraints.add("<http://dbpedia.org/ontology/serviceStartYear>");
      predConstraints.add("<http://dbpedia.org/ontology/engine>");
      predConstraints.add("<http://dbpedia.org/ontology/throwingSide>");
      predConstraints.add("<http://dbpedia.org/ontology/battingSide>");
      predConstraints.add("<http://dbpedia.org/ontology/affiliation>");
      predConstraints.add("<http://dbpedia.org/ontology/populationAsOf>");
      predConstraints.add("<http://dbpedia.org/ontology/year>");
      predConstraints.add("<http://dbpedia.org/ontology/foundingDate>");
      predConstraints.add("<http://dbpedia.org/ontology/formerName>");
      predConstraints.add("<http://dbpedia.org/ontology/leaderName>");
      predConstraints.add("<http://dbpedia.org/ontology/statisticValue>");
      predConstraints.add("<http://dbpedia.org/ontology/riverMouth>");
      predConstraints.add("<http://dbpedia.org/ontology/manufacturer>");
      predConstraints.add("<http://dbpedia.org/ontology/serviceEndYear>");
      predConstraints.add("<http://dbpedia.org/ontology/predecessor>");
      predConstraints.add("<http://dbpedia.org/ontology/displacement>");
      predConstraints.add("<http://dbpedia.org/ontology/statisticLabel>");
      predConstraints.add("<http://dbpedia.org/ontology/neighboringMunicipality>");
      predConstraints.add("<http://dbpedia.org/ontology/oclc>");
      predConstraints.add("<http://dbpedia.org/ontology/draftYear>");
      predConstraints.add("<http://dbpedia.org/ontology/routeTypeAbbreviation>");
      predConstraints.add("<http://dbpedia.org/ontology/routeNumber>");
      predConstraints.add("<http://dbpedia.org/ontology/spokenIn>");
      predConstraints.add("<http://dbpedia.org/ontology/restingPlace>");
      predConstraints.add("<http://dbpedia.org/ontology/broadcastStationClass>");
      predConstraints.add("<http://dbpedia.org/ontology/locationCountry>");
      predConstraints.add("<http://dbpedia.org/ontology/frequency>");
      predConstraints.add("<http://dbpedia.org/ontology/configuration>");
      predConstraints.add("<http://dbpedia.org/ontology/allegiance>");
      predConstraints.add("<http://dbpedia.org/ontology/shipLaunch>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfEmployees>");
      predConstraints.add("<http://dbpedia.org/ontology/bSide>");
      predConstraints.add("<http://dbpedia.org/ontology/railwayPlatforms>");
      predConstraints.add("<http://dbpedia.org/ontology/topSpeed>");
      predConstraints.add("<http://dbpedia.org/ontology/facilityId>");
      predConstraints.add("<http://dbpedia.org/ontology/college>");
      predConstraints.add("<http://dbpedia.org/ontology/routeStartDirection>");
      predConstraints.add("<http://dbpedia.org/ontology/routeEndDirection>");
      predConstraints.add("<http://dbpedia.org/ontology/education>");
      predConstraints.add("<http://dbpedia.org/ontology/mascot>");
      predConstraints.add("<http://dbpedia.org/ontology/architect>");
      predConstraints.add("<http://dbpedia.org/ontology/slogan>");
      predConstraints.add("<http://dbpedia.org/ontology/tenant>");
      predConstraints.add("<http://dbpedia.org/ontology/computingMedia>");
      predConstraints.add("<http://dbpedia.org/ontology/discoverer>");
      predConstraints.add("<http://dbpedia.org/ontology/coach>");
      predConstraints.add("<http://dbpedia.org/ontology/sourceCountry>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfSeasons>");
      predConstraints.add("<http://dbpedia.org/ontology/profession>");
      predConstraints.add("<http://dbpedia.org/ontology/militaryUnit>");
      predConstraints.add("<http://dbpedia.org/ontology/part>");
      predConstraints.add("<http://dbpedia.org/ontology/manager>");
      predConstraints.add("<http://dbpedia.org/ontology/commissioningDate>");
      predConstraints.add("<http://dbpedia.org/ontology/nearestCity>");
      predConstraints.add("<http://dbpedia.org/ontology/servingRailwayLine>");
      predConstraints.add("<http://dbpedia.org/ontology/budget>");
      predConstraints.add("<http://dbpedia.org/ontology/assembly>");
      predConstraints.add("<http://dbpedia.org/ontology/openingDate>");
      predConstraints.add("<http://dbpedia.org/ontology/orbitalPeriod>");
      predConstraints.add("<http://dbpedia.org/ontology/periapsis>");
      predConstraints.add("<http://dbpedia.org/ontology/related>");
      predConstraints.add("<http://dbpedia.org/ontology/entrezgene>");
      predConstraints.add("<http://dbpedia.org/ontology/apoapsis>");
      predConstraints.add("<http://dbpedia.org/ontology/operatedBy>");
      predConstraints.add("<http://dbpedia.org/ontology/formationYear>");
      predConstraints.add("<http://dbpedia.org/ontology/capacity>");
      predConstraints.add("<http://dbpedia.org/ontology/gross>");
      predConstraints.add("<http://dbpedia.org/ontology/epoch>");
      predConstraints.add("<http://dbpedia.org/ontology/broadcastNetwork>");
      predConstraints.add("<http://dbpedia.org/ontology/regionServed>");
      predConstraints.add("<http://dbpedia.org/ontology/date>");
      predConstraints.add("<http://dbpedia.org/ontology/absoluteMagnitude>");
      predConstraints.add("<http://dbpedia.org/ontology/firstAirDate>");
      predConstraints.add("<http://dbpedia.org/ontology/runwaySurface>");
      predConstraints.add("<http://dbpedia.org/ontology/discovered>");
      predConstraints.add("<http://dbpedia.org/ontology/runwayLength>");
      predConstraints.add("<http://dbpedia.org/ontology/result>");
      predConstraints.add("<http://dbpedia.org/ontology/iupacName>");
      predConstraints.add("<http://dbpedia.org/ontology/runwayDesignation>");
      predConstraints.add("<http://dbpedia.org/ontology/recordDate>");
      predConstraints.add("<http://dbpedia.org/ontology/operator>");
      predConstraints.add("<http://dbpedia.org/ontology/militaryCommand>");
      predConstraints.add("<http://dbpedia.org/ontology/influenced>");
      predConstraints.add("<http://dbpedia.org/ontology/province>");
      predConstraints.add("<http://dbpedia.org/ontology/foundationPlace>");
      predConstraints.add("<http://dbpedia.org/ontology/populationPlace>");
      predConstraints.add("<http://dbpedia.org/ontology/isPartOfMilitaryConflict>");
      predConstraints.add("<http://dbpedia.org/ontology/shipDraft>");
      predConstraints.add("<http://dbpedia.org/ontology/leaderFunction>");
      predConstraints.add("<http://dbpedia.org/ontology/owningCompany>");
      predConstraints.add("<http://dbpedia.org/ontology/arrondissement>");
      predConstraints.add("<http://dbpedia.org/ontology/layingDown>");
      predConstraints.add("<http://dbpedia.org/ontology/width>");
      predConstraints.add("<http://dbpedia.org/ontology/president>");
      predConstraints.add("<http://dbpedia.org/ontology/officialSchoolColour>");
      predConstraints.add("<http://dbpedia.org/ontology/child>");
      predConstraints.add("<http://dbpedia.org/ontology/percentageOfAreaWater>");
      predConstraints.add("<http://dbpedia.org/ontology/shoots>");
      predConstraints.add("<http://dbpedia.org/ontology/computingInput>");
      predConstraints.add("<http://dbpedia.org/ontology/ingredient>");
      predConstraints.add("<http://dbpedia.org/ontology/combatant>");
      predConstraints.add("<http://dbpedia.org/ontology/canton>");
      predConstraints.add("<http://dbpedia.org/ontology/completionDate>");
      predConstraints.add("<http://dbpedia.org/ontology/designer>");
      predConstraints.add("<http://dbpedia.org/ontology/foundedBy>");
      predConstraints.add("<http://dbpedia.org/ontology/channel>");
      predConstraints.add("<http://dbpedia.org/ontology/latestReleaseVersion>");
      predConstraints.add("<http://dbpedia.org/ontology/operatingSystem>");
      predConstraints.add("<http://dbpedia.org/ontology/composer>");
      predConstraints.add("<http://dbpedia.org/ontology/mountainRange>");
      predConstraints.add("<http://dbpedia.org/ontology/callsignMeaning>");
      predConstraints.add("<http://dbpedia.org/ontology/debutTeam>");
      predConstraints.add("<http://dbpedia.org/ontology/strength>");
      predConstraints.add("<http://dbpedia.org/ontology/license>");
      predConstraints.add("<http://dbpedia.org/ontology/company>");
      predConstraints.add("<http://dbpedia.org/ontology/icaoLocationIdentifier>");
      predConstraints.add("<http://dbpedia.org/ontology/parentCompany>");
      predConstraints.add("<http://dbpedia.org/ontology/numberBuilt>");
      predConstraints.add("<http://dbpedia.org/ontology/productionStartYear>");
      predConstraints.add("<http://dbpedia.org/ontology/bodyStyle>");
      predConstraints.add("<http://dbpedia.org/ontology/draftTeam>");
      predConstraints.add("<http://dbpedia.org/ontology/intercommunality>");
      predConstraints.add("<http://dbpedia.org/ontology/causalties>");
      predConstraints.add("<http://dbpedia.org/ontology/voice>");
      predConstraints.add("<http://dbpedia.org/ontology/primeMinister>");
      predConstraints.add("<http://dbpedia.org/ontology/draft>");
      predConstraints.add("<http://dbpedia.org/ontology/facultySize>");
      predConstraints.add("<http://dbpedia.org/ontology/presenter>");
      predConstraints.add("<http://dbpedia.org/ontology/ideology>");
      predConstraints.add("<http://dbpedia.org/ontology/executiveProducer>");
      predConstraints.add("<http://dbpedia.org/ontology/revenue>");
      predConstraints.add("<http://dbpedia.org/ontology/relatedMeanOfTransportation>");
      predConstraints.add("<http://dbpedia.org/ontology/season>");
      predConstraints.add("<http://dbpedia.org/ontology/formerCallsign>");
      predConstraints.add("<http://dbpedia.org/ontology/garrison>");
      predConstraints.add("<http://dbpedia.org/ontology/championInDoubleMale>");
      predConstraints.add("<http://dbpedia.org/ontology/episodeNumber>");
      predConstraints.add("<http://dbpedia.org/ontology/abbreviation>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfTracks>");
      predConstraints.add("<http://dbpedia.org/ontology/draftPick>");
      predConstraints.add("<http://dbpedia.org/ontology/draftRound>");
      predConstraints.add("<http://dbpedia.org/ontology/iataLocationIdentifier>");
      predConstraints.add("<http://dbpedia.org/ontology/commandStructure>");
      predConstraints.add("<http://dbpedia.org/ontology/governmentType>");
      predConstraints.add("<http://dbpedia.org/ontology/decommissioningDate>");
      predConstraints.add("<http://dbpedia.org/ontology/seasonNumber>");
      predConstraints.add("<http://dbpedia.org/ontology/productionEndYear>");
      predConstraints.add("<http://dbpedia.org/ontology/campus>");
      predConstraints.add("<http://dbpedia.org/ontology/issn>");
      predConstraints.add("<http://dbpedia.org/ontology/lcc>");
      predConstraints.add("<http://dbpedia.org/ontology/formationDate>");
      predConstraints.add("<http://dbpedia.org/ontology/relative>");
      predConstraints.add("<http://dbpedia.org/ontology/colour>");
      predConstraints.add("<http://dbpedia.org/ontology/owningOrganisation>");
      predConstraints.add("<http://dbpedia.org/ontology/trainer>");
      predConstraints.add("<http://dbpedia.org/ontology/academicDiscipline>");
      predConstraints.add("<http://dbpedia.org/ontology/mouthMountain>");
      predConstraints.add("<http://dbpedia.org/ontology/mouthPlace>");
      predConstraints.add("<http://dbpedia.org/ontology/licensee>");
      predConstraints.add("<http://dbpedia.org/ontology/prominence>");
      predConstraints.add("<http://dbpedia.org/ontology/agencyStationCode>");
      predConstraints.add("<http://dbpedia.org/ontology/previousEvent>");
      predConstraints.add("<http://dbpedia.org/ontology/doctoralStudent>");
      predConstraints.add("<http://dbpedia.org/ontology/chairman>");
      predConstraints.add("<http://dbpedia.org/ontology/teamName>");
      predConstraints.add("<http://dbpedia.org/ontology/colourName>");
      predConstraints.add("<http://dbpedia.org/ontology/layout>");
      predConstraints.add("<http://dbpedia.org/ontology/startDate>");
      predConstraints.add("<http://dbpedia.org/ontology/movement>");
      predConstraints.add("<http://dbpedia.org/ontology/daylightSavingTimeZone>");
      predConstraints.add("<http://dbpedia.org/ontology/relation>");
      predConstraints.add("<http://dbpedia.org/ontology/mouthElevation>");
      predConstraints.add("<http://dbpedia.org/ontology/leader>");
      predConstraints.add("<http://dbpedia.org/ontology/sourceMountain>");
      predConstraints.add("<http://dbpedia.org/ontology/sourcePlace>");
      predConstraints.add("<http://dbpedia.org/ontology/dcc>");
      predConstraints.add("<http://dbpedia.org/ontology/personName>");
      predConstraints.add("<http://dbpedia.org/ontology/role>");
      predConstraints.add("<http://dbpedia.org/ontology/automobileModel>");
      predConstraints.add("<http://dbpedia.org/ontology/powerOutput>");
      predConstraints.add("<http://dbpedia.org/ontology/sport>");
      predConstraints.add("<http://dbpedia.org/ontology/training>");
      predConstraints.add("<http://dbpedia.org/ontology/firstPublicationYear>");
      predConstraints.add("<http://dbpedia.org/ontology/startYearOfInsertion>");
      predConstraints.add("<http://dbpedia.org/ontology/iucnCategory>");
      predConstraints.add("<http://dbpedia.org/ontology/casNumber>");
      predConstraints.add("<http://dbpedia.org/ontology/overallRecord>");
      predConstraints.add("<http://dbpedia.org/ontology/notableCommander>");
      predConstraints.add("<http://dbpedia.org/ontology/wheelbase>");
      predConstraints.add("<http://dbpedia.org/ontology/portrayer>");
      predConstraints.add("<http://dbpedia.org/ontology/federalState>");
      predConstraints.add("<http://dbpedia.org/ontology/notableWork>");
      predConstraints.add("<http://dbpedia.org/ontology/transmission>");
      predConstraints.add("<http://dbpedia.org/ontology/citizenship>");
      predConstraints.add("<http://dbpedia.org/ontology/programmingLanguage>");
      predConstraints.add("<http://dbpedia.org/ontology/pubchem>");
      predConstraints.add("<http://dbpedia.org/ontology/ecNumber>");
      predConstraints.add("<http://dbpedia.org/ontology/frequencyOfPublication>");
      predConstraints.add("<http://dbpedia.org/ontology/lowerAge>");
      predConstraints.add("<http://dbpedia.org/ontology/militaryUnitSize>");
      predConstraints.add("<http://dbpedia.org/ontology/leaderParty>");
      predConstraints.add("<http://dbpedia.org/ontology/acquirementDate>");
      predConstraints.add("<http://dbpedia.org/ontology/service>");
      predConstraints.add("<http://dbpedia.org/ontology/floorCount>");
      predConstraints.add("<http://dbpedia.org/ontology/extinctionYear>");
      predConstraints.add("<http://dbpedia.org/ontology/atcPrefix>");
      predConstraints.add("<http://dbpedia.org/ontology/leftTributary>");
      predConstraints.add("<http://dbpedia.org/ontology/nonFictionSubject>");
      predConstraints.add("<http://dbpedia.org/ontology/upperAge>");
      predConstraints.add("<http://dbpedia.org/ontology/depth>");
      predConstraints.add("<http://dbpedia.org/ontology/employer>");
      predConstraints.add("<http://dbpedia.org/ontology/mouthPosition>");
      predConstraints.add("<http://dbpedia.org/ontology/endYearOfInsertion>");
      predConstraints.add("<http://dbpedia.org/ontology/rightTributary>");
      predConstraints.add("<http://dbpedia.org/ontology/goldMedalist>");
      predConstraints.add("<http://dbpedia.org/ontology/grades>");
      predConstraints.add("<http://dbpedia.org/ontology/managerTitle>");
      predConstraints.add("<http://dbpedia.org/ontology/monarch>");
      predConstraints.add("<http://dbpedia.org/ontology/nationalTopographicSystemMapNumber>");
      predConstraints.add("<http://dbpedia.org/ontology/doctoralAdvisor>");
      predConstraints.add("<http://dbpedia.org/ontology/silverMedalist>");
      predConstraints.add("<http://dbpedia.org/ontology/ethnicity>");
      predConstraints.add("<http://dbpedia.org/ontology/censusYear>");
      predConstraints.add("<http://dbpedia.org/ontology/bronzeMedalist>");
      predConstraints.add("<http://dbpedia.org/ontology/netIncome>");
      predConstraints.add("<http://dbpedia.org/ontology/isHandicappedAccessible>");
      predConstraints.add("<http://dbpedia.org/ontology/ceremonialCounty>");
      predConstraints.add("<http://dbpedia.org/ontology/jurisdiction>");
      predConstraints.add("<http://dbpedia.org/ontology/passengersPerYear>");
      predConstraints.add("<http://dbpedia.org/ontology/narrator>");
      predConstraints.add("<http://dbpedia.org/ontology/heightAboveAverageTerrain>");
      predConstraints.add("<http://dbpedia.org/ontology/variantOf>");
      predConstraints.add("<http://dbpedia.org/ontology/animator>");
      predConstraints.add("<http://dbpedia.org/ontology/headLabel>");
      predConstraints.add("<http://dbpedia.org/ontology/capital>");
      predConstraints.add("<http://dbpedia.org/ontology/veneratedIn>");
      predConstraints.add("<http://dbpedia.org/ontology/formerBroadcastNetwork>");
      predConstraints.add("<http://dbpedia.org/ontology/staff>");
      predConstraints.add("<http://dbpedia.org/ontology/championInSingleMale>");
      predConstraints.add("<http://dbpedia.org/ontology/games>");
      predConstraints.add("<http://dbpedia.org/ontology/faaLocationIdentifier>");
      predConstraints.add("<http://dbpedia.org/ontology/cost>");
      predConstraints.add("<http://dbpedia.org/ontology/pictureFormat>");
      predConstraints.add("<http://dbpedia.org/ontology/fdaUniiCode>");
      predConstraints.add("<http://dbpedia.org/ontology/editor>");
      predConstraints.add("<http://dbpedia.org/ontology/usedInWar>");
      predConstraints.add("<http://dbpedia.org/ontology/personFunction>");
      predConstraints.add("<http://dbpedia.org/ontology/parkingInformation>");
      predConstraints.add("<http://dbpedia.org/ontology/orderDate>");
      predConstraints.add("<http://dbpedia.org/ontology/hubAirport>");
      predConstraints.add("<http://dbpedia.org/ontology/callSign>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfUndergraduateStudents>");
      predConstraints.add("<http://dbpedia.org/ontology/circulation>");
      predConstraints.add("<http://dbpedia.org/ontology/followingEvent>");
      predConstraints.add("<http://dbpedia.org/ontology/sourcePosition>");
      predConstraints.add("<http://dbpedia.org/ontology/growingGrape>");
      predConstraints.add("<http://dbpedia.org/ontology/raceHorse>");
      predConstraints.add("<http://dbpedia.org/ontology/homeStadium>");
      predConstraints.add("<http://dbpedia.org/ontology/internationally>");
      predConstraints.add("<http://dbpedia.org/ontology/subsidiary>");
      predConstraints.add("<http://dbpedia.org/ontology/chairmanTitle>");
      predConstraints.add("<http://dbpedia.org/ontology/requirement>");
      predConstraints.add("<http://dbpedia.org/ontology/purpose>");
      predConstraints.add("<http://dbpedia.org/ontology/graySubject>");
      predConstraints.add("<http://dbpedia.org/ontology/grayPage>");
      predConstraints.add("<http://dbpedia.org/ontology/undraftedYear>");
      predConstraints.add("<http://dbpedia.org/ontology/gender>");
      predConstraints.add("<http://dbpedia.org/ontology/stationStructure>");
      predConstraints.add("<http://dbpedia.org/ontology/sourceElevation>");
      predConstraints.add("<http://dbpedia.org/ontology/countySeat>");
      predConstraints.add("<http://dbpedia.org/ontology/openingTheme>");
      predConstraints.add("<http://dbpedia.org/ontology/sex>");
      predConstraints.add("<http://dbpedia.org/ontology/identificationSymbol>");
      predConstraints.add("<http://dbpedia.org/ontology/effectiveRadiatedPower>");
      predConstraints.add("<http://dbpedia.org/ontology/diseasesdb>");
      predConstraints.add("<http://dbpedia.org/ontology/club>");
      predConstraints.add("<http://dbpedia.org/ontology/inflow>");
      predConstraints.add("<http://dbpedia.org/ontology/outflow>");
      predConstraints.add("<http://dbpedia.org/ontology/crosses>");
      predConstraints.add("<http://dbpedia.org/ontology/coverArtist>");
      predConstraints.add("<http://dbpedia.org/ontology/stylisticOrigin>");
      predConstraints.add("<http://dbpedia.org/ontology/floorArea>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfVolumes>");
      predConstraints.add("<http://dbpedia.org/ontology/dorlandsPrefix>");
      predConstraints.add("<http://dbpedia.org/ontology/dorlandsSuffix>");
      predConstraints.add("<http://dbpedia.org/ontology/magazine>");
      predConstraints.add("<http://dbpedia.org/ontology/deathCause>");
      predConstraints.add("<http://dbpedia.org/ontology/meshId>");
      predConstraints.add("<http://dbpedia.org/ontology/fate>");
      predConstraints.add("<http://dbpedia.org/ontology/species>");
      predConstraints.add("<http://dbpedia.org/ontology/fareZone>");
      predConstraints.add("<http://dbpedia.org/ontology/passengersPerDay>");
      predConstraints.add("<http://dbpedia.org/ontology/plays>");
      predConstraints.add("<http://dbpedia.org/ontology/mainInterest>");
      predConstraints.add("<http://dbpedia.org/ontology/municipalityCode>");
      predConstraints.add("<http://dbpedia.org/ontology/nextEvent>");
      predConstraints.add("<http://dbpedia.org/ontology/firstAppearance>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfPostgraduateStudents>");
      predConstraints.add("<http://dbpedia.org/ontology/seatingCapacity>");
      predConstraints.add("<http://dbpedia.org/ontology/watershed>");
      predConstraints.add("<http://dbpedia.org/ontology/otherServingLines>");
      predConstraints.add("<http://dbpedia.org/ontology/meaning>");
      predConstraints.add("<http://dbpedia.org/ontology/ratio>");
      predConstraints.add("<http://dbpedia.org/ontology/totalPopulation>");
      predConstraints.add("<http://dbpedia.org/ontology/publicationDate>");
      predConstraints.add("<http://dbpedia.org/ontology/certification>");
      predConstraints.add("<http://dbpedia.org/ontology/localAuthority>");
      predConstraints.add("<http://dbpedia.org/ontology/atcSuffix>");
      predConstraints.add("<http://dbpedia.org/ontology/symbol>");
      predConstraints.add("<http://dbpedia.org/ontology/vehicleCode>");
      predConstraints.add("<http://dbpedia.org/ontology/digitalChannel>");
      predConstraints.add("<http://dbpedia.org/ontology/assets>");
      predConstraints.add("<http://dbpedia.org/ontology/domain>");
      predConstraints.add("<http://dbpedia.org/ontology/careerPrizeMoney>");
      predConstraints.add("<http://dbpedia.org/ontology/operatingIncome>");
      predConstraints.add("<http://dbpedia.org/ontology/populationUrban>");
      predConstraints.add("<http://dbpedia.org/ontology/illustrator>");
      predConstraints.add("<http://dbpedia.org/ontology/impactFactorAsOf>");
      predConstraints.add("<http://dbpedia.org/ontology/impactFactor>");
      predConstraints.add("<http://dbpedia.org/ontology/museum>");
      predConstraints.add("<http://dbpedia.org/ontology/dissolutionYear>");
      predConstraints.add("<http://dbpedia.org/ontology/bridgeCarries>");
      predConstraints.add("<http://dbpedia.org/ontology/omim>");
      predConstraints.add("<http://dbpedia.org/ontology/municipality>");
      predConstraints.add("<http://dbpedia.org/ontology/closingYear>");
      predConstraints.add("<http://dbpedia.org/ontology/grandsire>");
      predConstraints.add("<http://dbpedia.org/ontology/trackNumber>");
      predConstraints.add("<http://dbpedia.org/ontology/governor>");
      predConstraints.add("<http://dbpedia.org/ontology/person>");
      predConstraints.add("<http://dbpedia.org/ontology/gameEngine>");
      predConstraints.add("<http://dbpedia.org/ontology/torqueOutput>");
      predConstraints.add("<http://dbpedia.org/ontology/parentMountainPeak>");
      predConstraints.add("<http://dbpedia.org/ontology/largestCity>");
      predConstraints.add("<http://dbpedia.org/ontology/basedOn>");
      predConstraints.add("<http://dbpedia.org/ontology/homeport>");
      predConstraints.add("<http://dbpedia.org/ontology/diameter>");
      predConstraints.add("<http://dbpedia.org/ontology/sire>");
      predConstraints.add("<http://dbpedia.org/ontology/militaryRank>");
      predConstraints.add("<http://dbpedia.org/ontology/televisionSeries>");
      predConstraints.add("<http://dbpedia.org/ontology/otherParty>");
      predConstraints.add("<http://dbpedia.org/ontology/emedicineSubject>");
      predConstraints.add("<http://dbpedia.org/ontology/emedicineTopic>");
      predConstraints.add("<http://dbpedia.org/ontology/firstLeader>");
      predConstraints.add("<http://dbpedia.org/ontology/school>");
      predConstraints.add("<http://dbpedia.org/ontology/councilArea>");
      predConstraints.add("<http://dbpedia.org/ontology/archipelago>");
      predConstraints.add("<http://dbpedia.org/ontology/politicalPartyInLegislature>");
      predConstraints.add("<http://dbpedia.org/ontology/governingBody>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfStations>");
      predConstraints.add("<http://dbpedia.org/ontology/powerType>");
      predConstraints.add("<http://dbpedia.org/ontology/territory>");
      predConstraints.add("<http://dbpedia.org/ontology/significantBuilding>");
      predConstraints.add("<http://dbpedia.org/ontology/depictionDescription>");
      predConstraints.add("<http://dbpedia.org/ontology/aSide>");
      predConstraints.add("<http://dbpedia.org/ontology/saint>");
      predConstraints.add("<http://dbpedia.org/ontology/specialist>");
      predConstraints.add("<http://dbpedia.org/ontology/lccn>");
      predConstraints.add("<http://dbpedia.org/ontology/secondLeader>");
      predConstraints.add("<http://dbpedia.org/ontology/highschool>");
      predConstraints.add("<http://dbpedia.org/ontology/discharge>");
      predConstraints.add("<http://dbpedia.org/ontology/numberOfLocations>");
      predConstraints.add("<http://www.w3.org/2004/02/skos/core#subject>");
      predConstraints.add("<http://dbpedia.org/ontology/membership>");
      predConstraints.add("<http://dbpedia.org/ontology/formerCoach>");
      predConstraints.add("<http://dbpedia.org/ontology/source>");
      predConstraints.add("<http://dbpedia.org/ontology/pseudonym>");
      predConstraints.add("<http://dbpedia.org/ontology/tennisSurfaceType>");
      predConstraints.add("<http://dbpedia.org/ontology/hybrid>");
      predConstraints.add("<http://dbpedia.org/ontology/parentOrganisation>");
      predConstraints.add("<http://dbpedia.org/ontology/formerChannel>");
      predConstraints.add("<http://dbpedia.org/ontology/endowment>");
      predConstraints.add("<http://dbpedia.org/ontology/albedo>");
      predConstraints.add("<http://dbpedia.org/ontology/damsire>");
      predConstraints.add("<http://dbpedia.org/ontology/fuel>");
      predConstraints.add("<http://dbpedia.org/ontology/populationMetro>");
      predConstraints.add("<http://dbpedia.org/ontology/shipDisplacement>");
      predConstraints.add("<http://dbpedia.org/ontology/campusType>");
      predConstraints.add("<http://dbpedia.org/ontology/sales>");
      predConstraints.add("<http://dbpedia.org/ontology/billed>");
      predConstraints.add("<http://dbpedia.org/ontology/bicycleInformation>");
      predConstraints.add("<http://dbpedia.org/ontology/lineLength>");
      predConstraints.add("<http://dbpedia.org/ontology/picture>");
      predConstraints.add("<http://dbpedia.org/ontology/bedCount>");
      predConstraints.add("<http://dbpedia.org/ontology/lastAppearance>");
      predConstraints.add("<http://dbpedia.org/ontology/showJudge>");
      predConstraints.add("<http://dbpedia.org/ontology/category>");
      predConstraints.add("<http://dbpedia.org/ontology/institution>");
      predConstraints.add("<http://dbpedia.org/ontology/firstAscentYear>");
      predConstraints.add("<http://dbpedia.org/ontology/schoolBoard>");
      predConstraints.add("<http://dbpedia.org/ontology/breeder>");
      predConstraints.add("<http://dbpedia.org/ontology/visitorStatisticsAsOf>");
      predConstraints.add("<http://dbpedia.org/ontology/automobilePlatform>");
      predConstraints.add("<http://dbpedia.org/ontology/drugbank>");
      predConstraints.add("<http://dbpedia.org/ontology/appointer>");
      predConstraints.add("<http://dbpedia.org/ontology/championInDoubleFemale>");
      predConstraints.add("<http://dbpedia.org/ontology/equity>");
      predConstraints.add("<http://dbpedia.org/ontology/distanceToLondon>");
      predConstraints.add("<http://dbpedia.org/ontology/honours>");
      predConstraints.add("<http://dbpedia.org/ontology/philosophicalSchool>");
      predConstraints.add("<http://dbpedia.org/ontology/mission>");
      predConstraints.add("<http://dbpedia.org/ontology/populationTotalRanking>");
      predConstraints.add("<http://dbpedia.org/ontology/imageSize>");
      predConstraints.add("<http://dbpedia.org/ontology/electionMajority>");
      predConstraints.add("<http://dbpedia.org/ontology/medlineplus>");
      predConstraints.add("<http://dbpedia.org/ontology/lieutenant>");
      predConstraints.add("<http://dbpedia.org/ontology/deputy>");
      predConstraints.add("<http://dbpedia.org/ontology/distributingCompany>");
      predConstraints.add("<http://dbpedia.org/ontology/distributingLabel>");
      predConstraints.add("<http://dbpedia.org/ontology/majorShrine>");
      predConstraints.add("<http://dbpedia.org/ontology/editorTitle>");
      predConstraints.add("<http://dbpedia.org/ontology/lyrics>");
      predConstraints.add("<http://dbpedia.org/ontology/rebuildingYear>");
    } else if (pc.equals(PredConstraint.D13)) {
      // P13
      predConstraints.add("<http://dbpedia.org/ontology/country>");
      predConstraints.add("<http://dbpedia.org/ontology/division>");
      predConstraints.add("<http://dbpedia.org/ontology/genus>");
      predConstraints.add("<http://dbpedia.org/ontology/isPartOf>");
      predConstraints.add("<http://dbpedia.org/ontology/phylum>");
      predConstraints.add("<http://dbpedia.org/ontology/spokenIn>");
      predConstraints.add("<http://dbpedia.org/ontology/timeZone>");
      predConstraints.add("<http://dbpedia.org/ontology/wikiPageExternalLink>");
      predConstraints.add("<http://dbpedia.org/ontology/wikiPageRedirects>");
      predConstraints.add("<http://dbpedia.org/ontology/wikiPageWikiLink>");
      predConstraints.add("<http://dbpedia.org/property/hasPhotoCollection>");
      predConstraints.add("<http://purl.org/dc/terms/subject>");
      predConstraints.add("<http://www.w3.org/2002/07/owl#sameAs>");
    }
  }

  public static void main(String[] args) {
    final GraphChiVSMainVerExperimentNDMinimized m = new GraphChiVSMainVerExperimentNDMinimized();
    m.run(args);
  }

  private void preprocess(String graphName, int shards, Reader is) {
    int tcount = 1;
    final Timer tp = new Timer("preprocess-all", true);
    Timer t = new Timer("preprocess-" + tcount++, true);
    int c = 0;
    try {
      final FastSharder<NodeFBDataMinimized, EdgeFBDataMinimized> sharder = createSharder(graphName, shards);
      //			sharder.shard(System.in, "edgelist");
      final BufferedReader br;
      if (!(is instanceof BufferedReader)) {
        br = new BufferedReader(is);
      } else {
        br = (BufferedReader) is;
      }
      String line;
      while ((line = br.readLine()) != null) {
        if (linelimit-- == 0) {
          break;
        }
        if (line.length() > 0) {
          final String parse[] = line.split("\t| ");
          if (parse.length < 3) {
            System.err.println("ERROR: invalid line found: " + line);
            continue;
          }
          //					simplelogger.log("line:" + line);

          final String s = parse[0];
          final String p = parse[1];
          final String o = parse[2];

          if (flagFilterPreds && !predConstraints.contains(p)) {
            continue;
          }

          if (flagIntIn) {
            try {
              final int si = Integer.parseInt(s);
              final int pi = Integer.parseInt(p);
              final int oi = Integer.parseInt(o);

              if (si != oi) {
                sharder.addEdge(si, oi, pi + "");
              }
            } catch (NumberFormatException nfe) {
            }
          } else {

            // apply filters based on predicate
//					int checkLimit = 5;
//					String pt = p.replaceAll("<", "").replaceAll(">", "");
//					if (-1 == dc.lmdb.indexOf(pt) || checkLimit  < (dc.lmdb.indexOf(pt)+1)) continue;
            Integer s_nid = nodeIds.get(s);
            if (s_nid == null) {
              s_nid = nodeid_currentindex;
              nodeIds.put(s, s_nid);
              nodeid_currentindex++;
            }
            Integer p_pid = predIds2.get(p);
            if (p_pid == null) {
              p_pid = predid_currentindex;
              predIds2.put(p, p_pid);
              predid_currentindex++;
            }
            Integer o_nid = nodeIds.get(o);
            if (o_nid == null) {
              o_nid = nodeid_currentindex;
              nodeIds.put(o, o_nid);
              nodeid_currentindex++;
            }
//					System.out.println(s_nid + " " + p_pid + " " + o_nid);
            final int sv = s_nid;
            final int tv = o_nid;

            if (sv != tv) {
              sharder.addEdge(sv, tv, p_pid + "");
            }

          }
					//					if (sv == tv) {
          //						simplelogger.log("sv:" + sv);
          //					}

          // trigger a node's default hash
//					sharder.addEdge(sv, sv, "");
//					sharder.addEdge(tv, tv, "");
          c++;
          if (c % 1000000 == 0) {
            simplelogger.log("at line:" + c);
            simplelogger.log(t.stop().toString());
            simplelogger.log(tp.toString());
            t = new Timer("preprocess-" + tcount++, true);
          }
//					if (c % 50000000 == 0) {
//						DBmaker.commit();
//					}
        }
      }
      br.close();
      simplelogger.log("total lines:" + c);
      sharder.process();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  protected static FastSharder<NodeFBDataMinimized, EdgeFBDataMinimized> createSharder(String graphName, int numShards) throws IOException {
    return new FastSharder<NodeFBDataMinimized, EdgeFBDataMinimized>(graphName, numShards, new VertexProcessor<NodeFBDataMinimized>() {
      @Override
      public NodeFBDataMinimized receiveVertexValue(int vertexId, String token) {
        final NodeFBDataMinimized ed = new NodeFBDataMinimized();
//				ed.doInit();
        ed.nodeid = vertexId;
        //				if (token != null) {
        //									ed.nodeid = Integer.parseInt(token);
        //				} else {
        //					ed.nodeid = 0;
        //				}
        //				simplelogger.log("ed.h:" + vertexId + ":" + DigestUtils.md5Hex(ed.h));
        //				if (token != null) {
        //					ed.h[0] = 0x1;
        //				}
        return ed;
      }
    }, new EdgeProcessor<EdgeFBDataMinimized>() {
      @Override
      public EdgeFBDataMinimized receiveEdge(int from, int to, String token) {
        final EdgeFBDataMinimized ed = new EdgeFBDataMinimized();
//				ed.doInit();
        if (token != null) {
          ed.predicateid = Integer.parseInt(token);
        }
        return ed;
      }
    }, new NodeFBDataMinimizedConverter(), new EdgeFBDataMinimizedConverter());
  }
}
