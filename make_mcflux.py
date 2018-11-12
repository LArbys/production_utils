import sys

print "-----------------------"
print "| DL mcflux maker     |"
print "-----------------------"

if len(sys.argv) != 3:
    print ""
    print "SAMPLE         = str(sys.argv[1])"
    print "WEIGHT_PREFIX  = str(sys.argv[2])"
    print ""
    sys.exit(1)

import os, time, subprocess, csv, getpass
from lib.db import get_stage1_flist

def exec_system(input_):
    return subprocess.Popen(input_,stdout=subprocess.PIPE).communicate()[0].split("\n")[:-1]

def njobs():
    ret = exec_system(["squeue","-u",getpass.getuser()])
    ret = [r for r in ret if "interacti" in r]
    return int(len(ret))

def main() :

    SAMPLE         = str(sys.argv[1])
    WEIGHT_PREFIX  = str(sys.argv[2])

    DB_DIR = "/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db"

    #
    # execute
    #
    CONTAINER  = "/cluster/kappa/90-days-archive/wongjiradlab/vgenty/images/"
    CONTAINER += "evweight_uboonecode_11092018.img"
    
    for flist in get_stage1_flist(SAMPLE,0):

        MCINFO = flist['mcinfo']
        MCINFO = os.path.realpath(MCINFO)
                
        OUTDIR = os.path.dirname(MCINFO)
        OUTDIR = OUTDIR.replace("stage1","stage2/%s" % WEIGHT_PREFIX)

        JOBTAG = str(flist['jobtag'])

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
        SS += "python /usr/local/share/sw/scripts/sbnfit/dump_mcinfo/dump_E_and_L.py %s %s %s 1>/dev/null 2>/dev/null"
        SS += "\""
        SS += " &"    
        SS = SS % (CONTAINER, 
                   MCINFO.replace("90-days-archive",""),
                   JOBTAG,
                   OUTDIR.replace("90-days-archive",""))


        print SS
        os.system(SS)
        while 1:
            if njobs() >= 100:
                print "Sleeping"
                time.sleep(1)
            else:
                break

    return

if __name__ == '__main__':
    main()
    print
    print
    print "COMPLETE!"
    print "COMPLETE!"
    print "COMPLETE!"
    print
    print
    sys.exit(0)

