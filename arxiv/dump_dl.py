import os,sys

if len(sys.argv) != 9:
    print ""
    print "SAMPLE = str(sys.argv[1])"
    print "http://nudot.lns.mit.edu/taritree/dlleepubsummary.html"
    print
    print "TYPE   = int(sys.argv[2])"
    print "0 == RGB 1 == SSNET"
    print
    print "RUNTAG = str(sys.argv[3])"
    print "RUN    = int(sys.argv[4])"
    print "SUBRUN = int(sys.argv[5])"
    print "EVENT  = int(sys.argv[6])"
    print "VTXID  = int(sys.argv[7])"
    print 
    print "OUTPUT = str(sys.argv[8])"
    print ""
    sys.exit(1)
    
    
SAMPLE = str(sys.argv[1])
TYPE   = int(sys.argv[2])
runtag = str(sys.argv[3])
RUN    = int(sys.argv[4])
SUBRUN = int(sys.argv[5])
EVENT  = int(sys.argv[6])
VTXID  = int(sys.argv[7])
OUTPUT = str(sys.argv[8])

OUTPUT = os.path.join(os.getcwd(),OUTPUT)

type_ = ""
if TYPE == 0:
    type_ = "img"
else:
    type_ = "ssnet"


STAGE1_DIR = os.path.join("/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db",
                          SAMPLE,"stage1")

if "cocktail" in SAMPLE:
    STAGE1_DIR = os.path.join("/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db",
                              SAMPLE)
if "corsika" in SAMPLE:
    STAGE1_DIR = os.path.join("/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db",
                              SAMPLE)

STAGE2_DIR = os.path.join("/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db",
                          SAMPLE,"stage2",runtag)

PKL_FILE = os.path.join("/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db/rse/samples",
                        SAMPLE,"comb.pkl")

if os.path.exists(PKL_FILE) == False:
    print "NO rse file is available for this sample"
    print "        please contact vic              "
    print "         skype:@vicgenty                "
    sys.exit(1)

CONTAINER  = "/cluster/kappa/90-days-archive/wongjiradlab/vgenty/vertex/vertex_04092018_p00/image/"
CONTAINER += "singularity-dllee-unified-04092018_revert.img"

SS = "module load singularity && srun -p interactive --job-name rse singularity exec %s bash -c \"source /usr/local/bin/thisroot.sh 1>/dev/null 2>/dev/null && cd /usr/local/share/dllee_unified/ 1>/dev/null 2>/dev/null && source configure.sh 1>/dev/null 2>/dev/null && python /usr/local/share/dllee_unified/LArCV/app/LArOpenCVHandle/ana/dump_img/dump_%s.py %s %s %s %d %d %d %d %s\""

SS = SS % (CONTAINER, 
           type_, 
           PKL_FILE.replace("90-days-archive",""),
           STAGE1_DIR.replace("90-days-archive",""),
           STAGE2_DIR.replace("90-days-archive",""), 
           RUN, 
           SUBRUN,
           EVENT, 
           VTXID, 
           OUTPUT.replace("90-days-archive",""))

print "Executing @ interactive node type=%s" % type_
os.system(SS)

sys.exit(0)
