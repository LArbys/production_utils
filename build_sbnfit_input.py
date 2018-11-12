import sys

print "------------------------"
print "| SBNFIT INPUT BUILDER |"
print "------------------------"

if len(sys.argv) != 10:
    print ""
    print "NUE_INTRINSIC_EVENTLIST = str(sys.argv[1])"
    print "NUE_BNB_EVENTLIST       = str(sys.argv[2])"
    print "NUE_EXTBNB_EVENTLIST    = str(sys.argv[3])"
    print "NUMU_BNB_EVENTLIST      = str(sys.argv[4])"
    print "NUMU_EXTBNB_EVENTLIST   = str(sys.argv[5])"
    print
    print "NUE_INTRINSIC_SAMPLE    = str(sys.argv[6])"
    print "BNB_SAMPLE1             = str(sys.argv[7])"
    print "WEIGHT_PREFIX           = str(sys.argv[8])"
    print "OUTPUT                  = str(sys.argv[9])"
    print ""
    sys.exit(1)

import os, time, subprocess, csv, getpass

def exec_system(input_):
    return subprocess.Popen(input_,stdout=subprocess.PIPE).communicate()[0].split("\n")[:-1]

def njobs():
    ret = exec_system(["squeue","-u",getpass.getuser()])
    ret = [r for r in ret if "interacti" in r]
    return int(len(ret))

NUE_INTRINSIC_EVENTLIST = str(sys.argv[1])
NUE_BNB_EVENTLIST       = str(sys.argv[2])
NUE_EXTBNB_EVENTLIST    = str(sys.argv[3])
NUMU_BNB_EVENTLIST      = str(sys.argv[4])
NUMU_EXTBNB_EVENTLIST   = str(sys.argv[5])
NUE_INTRINSIC_SAMPLE    = str(sys.argv[6])
BNB_SAMPLE1             = str(sys.argv[7])
WEIGHT_PREFIX           = str(sys.argv[8])
OUTPUT                  = str(sys.argv[9])

OUTPUT = os.path.join(os.getcwd(),OUTPUT)

DB_DIR = "/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db"

NUE_SBNFIT_DIR  = os.path.join(DB_DIR,NUE_INTRINSIC_SAMPLE,"stage2",WEIGHT_PREFIX,"sbnfit")
NUMU_SBNFIT_DIR = os.path.join(DB_DIR,BNB_SAMPLE1         ,"stage2",WEIGHT_PREFIX,"sbnfit")

NUE_WEIGHT_FILE = os.path.join(NUE_SBNFIT_DIR ,"arborist_evweight_art.root")
BNB_WEIGHT_FILE = os.path.join(NUMU_SBNFIT_DIR,"arborist_evweight_art.root")

NUE_FINAL_FILE  = os.path.join(NUE_SBNFIT_DIR ,"andy_out.root")
NUMU_FINAL_FILE = os.path.join(NUMU_SBNFIT_DIR,"andy_out.root")

NUE_MCFLUX_FILE  = os.path.join(NUE_SBNFIT_DIR ,"mcflux_dump.txt")
NUMU_MCFLUX_FILE = os.path.join(NUMU_SBNFIT_DIR,"mcflux_dump.txt")


#
# execute
#
CONTAINER  = "/cluster/kappa/90-days-archive/wongjiradlab/vgenty/images/"
CONTAINER += "evweight_uboonecode_11092018.img"

SS  = ""
SS += "srun -p interactive --mem=2000 --job-name mcflux singularity exec %s bash -c "
SS += "\""
SS += "source /products/setup 1>/dev/null 2>/dev/null"
SS += " && "
SS += "setup uboonecode v06_26_01_24 -q e10:prof 1>/dev/null 2>/dev/null"
SS += " && "
SS += "setup mrb 1>/dev/null 2>/dev/null"
SS += " && "
SS += "export MRB_PROJECT=larsoft 1>/dev/null 2>/dev/null"
SS += " && "
SS += "cd /usr/local/share/ew/ 1>/dev/null 2>/dev/null"
SS += " && "
SS += "source localProducts_larsoft_v06_26_01_15_e10_prof/setup 1>/dev/null 2>/dev/null"
SS += " && "
SS += "source fix_build.sh 1>/dev/null 2>/dev/null"
SS += " && "
SS += "cd /usr/local/share/sw/ 1>/dev/null 2>/dev/null"
SS += " && "
SS += "source larlite/config/setup.sh 1>/dev/null 2>/dev/null"
SS += " && "
SS += "/usr/local/share/sw/scripts/sbnfit/make_sbnfit_input/make_sbnfit_input %s %s %s %s %s %s %s %s %s %s %s %s"
SS += "\""

SS = SS % (CONTAINER, 
           os.path.realpath(NUE_INTRINSIC_EVENTLIST).replace("90-days-archive",""),
           os.path.realpath(NUE_BNB_EVENTLIST).replace("90-days-archive",""),
           os.path.realpath(NUE_EXTBNB_EVENTLIST).replace("90-days-archive",""),
           os.path.realpath(NUMU_BNB_EVENTLIST).replace("90-days-archive",""),
           os.path.realpath(NUMU_EXTBNB_EVENTLIST).replace("90-days-archive",""),
           os.path.realpath(os.path.join(OUTPUT,"input_to_sbnfit.root")).replace("90-days-archive",""),
           NUE_WEIGHT_FILE.replace("90-days-archive",""),
           BNB_WEIGHT_FILE.replace("90-days-archive",""),
           NUE_FINAL_FILE.replace("90-days-archive",""),
           NUMU_FINAL_FILE.replace("90-days-archive",""),
           NUE_MCFLUX_FILE.replace("90-days-archive",""),
           NUMU_MCFLUX_FILE.replace("90-days-archive",""))
           
           
print SS
os.system(SS)
