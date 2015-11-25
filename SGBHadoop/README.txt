* FW bisimulation class

edu.toronto.cs.vinosem.hadoop220.BisimFWMPCInted - FW bisimulation using inted input. This is the class that is used to compare in experiments with GraphChi (since the research literature only deals with FW summaries). The next few classes construct FWBW summaries which can be compared to the GraphChi-based implementation.
edu.toronto.cs.vinosem.hadoop220.BisimFWBWMPCInted - FWBW bisimulation using inted input
edu.toronto.cs.vinosem.hadoop220.BisimFWBWMPC - FW bisimulation using reduce only to sort (block ids are computed in map task for more parallelism)
edu.toronto.cs.vinosem.hadoop220.BisimFWBW - FWBW bisimulation using string input (block ids are computed in reduce task for simplicity)

* Command line properties

args[0] - Input folder
args[1] - Output folder

* Properties that can be passed in via -D

# Number of mappers
-Dvssp.nummaps=4 

# Hadoop home dir
-Dhadoop.home.dir=/Hadoop-2.2.0/hadoop-2.2.0 
-Djava.library.path=c:/workspace_helios_vinosem/Hadoop-2.2.0/hadoop-2.2.0/bin 
-Dhadoop.tmp.dir=c:/htmp 

* Example

java -Dvssp.nummaps=4 -Dhadoop.home.dir=/Hadoop-2.2.0/hadoop-2.2.0 -Djava.library.path=/Hadoop-2.2.0/hadoop-2.2.0/bin -Dhadoop.tmp.dir=/htmp -classpath %classpath edu.toronto.cs.vinosem.hadoop220.BisimFWMPCInted /data/input/inputfile.gz /data/outputfolder