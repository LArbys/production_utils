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

os.system("mkdir -p out/%s" % (SAMPLE))

CONTAINER  = "/cluster/kappa/90-days-archive/wongjiradlab/vgenty/vertex/vertex_03132018_p00/image/"
CONTAINER += "singularity-dllee-unified-03132018_revert.img"

OUTDIR = "/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db/rse/out/%s/" % SAMPLE
OUTDIR = OUTDIR.replace("90-days-archive","")

for flist in get_stage1_flist(SAMPLE,IS_MC):
    OPRECO = flist['opreco']
    OPRECO = OPRECO.replace("90-days-archive","")

    SS = "srun -p interactive --job-name rse singularity exec %s bash -c \"source /usr/local/bin/thisroot.sh 1>/dev/null 2>/dev/null && python /usr/local/share/dllee_unified/LArCV/app/LArOpenCVHandle/ana/dump_rse.py %s %s 1>/dev/null 2>/dev/null\""
    SS = SS % (CONTAINER,OPRECO,OUTDIR)
    SS += " &"
    print SS
    os.system(SS)

    while 1:
        if njobs() >= 50:
            print "Sleeping"
            time.sleep(1)
        else:
            break

    
    
