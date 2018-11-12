import sys, os,time, subprocess, getpass
from lib.db import *

def exec_system(input_):
    return subprocess.Popen(input_,stdout=subprocess.PIPE).communicate()[0].split("\n")[:-1]

def njobs():
    ret = exec_system(["squeue","-u",getpass.getuser()])
    ret = [r for r in ret if "interacti" in r]
    return int(len(ret))

SAMPLE = str(sys.argv[1])
IS_MC  = int(sys.argv[2])

os.system("mkdir -p check_ssnet/%s" % (SAMPLE))

CONTAINER  = "/cluster/kappa/90-days-archive/wongjiradlab/vgenty/vertex/vertex_09242018_p00/image/"
CONTAINER += "singularity-dllee-unified-09242018_test21.img"

OUTDIR = "/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db/rse/check_ssnet/%s/" % SAMPLE
OUTDIR = OUTDIR.replace("90-days-archive","")

for flist in get_stage1_flist(SAMPLE,IS_MC):
    FNAME1 = os.path.realpath(flist['supera'])
    # FNAME2 = os.path.realpath(flist['taggerout-larcv'])
    # FNAME3 = os.path.realpath(flist['ssnetserverout-larcv'])

    FNAME2 = os.path.realpath(flist['taggeroutv2-larcv'])
    FNAME3 = os.path.realpath(flist['ssnetserveroutv2-larcv'])

    FNAME1 = FNAME1.replace("90-days-archive","")
    FNAME2 = FNAME2.replace("90-days-archive","")
    FNAME3 = FNAME3.replace("90-days-archive","")

    SS  = "srun -p interactive --job-name mcinfo singularity exec %s bash -c \""
    SS += "source /usr/local/bin/thisroot.sh 1>/dev/null 2>/dev/null " 
    SS += "&& "
    SS += "cd /usr/local/share/dllee_unified/ 1>/dev/null 2>/dev/null " 
    SS += "&& "
    SS += "source configure.sh 1>/dev/null 2>/dev/null "
    SS += "&& "
    SS += "python /usr/local/share/dllee_unified/LArCV/app/LArOpenCVHandle/ana/ssnet_check/check_ssnet.py %s %s %s %s %s %s 1>/dev/null 2>/dev/null\""
    SS = SS % (CONTAINER,
               FNAME1,
               FNAME2,
               FNAME3,
               "1",
               flist['jobtag'],
               OUTDIR)
    SS += " &"
    print SS
    os.system(SS)
    while 1:
        if njobs() >= 100:
            print "Sleeping"
            time.sleep(1)
        else:
            break


    
