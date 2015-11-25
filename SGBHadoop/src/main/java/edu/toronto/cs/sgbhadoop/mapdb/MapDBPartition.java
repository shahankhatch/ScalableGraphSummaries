package edu.toronto.cs.sgbhadoop.mapdb;

import edu.toronto.cs.sgbhadoop.util.FileUtil;
import edu.toronto.cs.sgbhadoop.util.SimpleLogger;
import edu.toronto.cs.sgbhadoop.util.Timer;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

public class MapDBPartition {
    static SimpleLogger simplelogger = new SimpleLogger();

    public static void main(String[] args) throws Exception {
        try {
            simplelogger.setup("MapDBPartition");
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Problems with creating the log files");
        }
        if (args.length != 1) {
            System.err.println("args: <nodeblockmapfile>");
            System.exit(1);
        }
        simplelogger.log("args:" + args[0]);
        String filestring = args[0];
        MapDBPartition mapDBPartition = new MapDBPartition();
        simplelogger.log("starting to create db");
        mapDBPartition.create(filestring);
        simplelogger.log("starting to test db");
        mapDBPartition.test();
    }

    DB make;
    HTreeMap<String, String> hashMap;

    public MapDBPartition() {
        make = DBMaker.newFileDB(new File("partitioncollectiondb")).closeOnJvmShutdown().compressionEnable().make();
        getHashMap();
    }

    public HTreeMap<String, String> getHashMap() {
        hashMap = make.createHashMap("partitioncollectionhashmap").make();
        return hashMap;
    }

    private void test() {

        Timer t = new Timer("mapdb-test", true);
        Set<String> keySet = hashMap.keySet();
        int i = 0;
        for (String s : keySet) {
            if (++i % 1000000 == 0) {
                simplelogger.log("linecheck:" + (i - 1));
                simplelogger.log(t.toString());
            }
            if (i % 100000 == 0) {
                simplelogger.log("entry: " + s + ", " + hashMap.get(s));
            }
        }
        simplelogger.log(t.stop().toString());
    }

    public void create(String filestring) throws Exception {

        Timer t = new Timer("mapdb-create", true);
        BufferedReader br = FileUtil.getBufferedReader(filestring);
        String line = "";
        int i = 0;
        while ((line = br.readLine()) != null) {
            //			if (i > 1000)
            //				break;
            if (++i % 1000000 == 0) {
                simplelogger.log("line:" + i);
                simplelogger.log(t.toString());
                make.commit();
            }
            String split[] = line.split("\t| ");
            // node it, block id
            hashMap.put(split[0], split[1]);
        }
        simplelogger.log("totallines:" + i);
        simplelogger.log(t.stop().toString());
        make.commit();

        simplelogger.log("hashmap-size:" + hashMap.sizeLong());
    }
}
