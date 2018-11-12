import sys

print "-----------------------"
print "| DL arborist maker   |"
print "-----------------------"

if len(sys.argv) != 3:
    print ""
    print "SAMPLE         = str(sys.argv[1])"
    print "WEIGHT_PREFIX  = str(sys.argv[2])"
    print ""
    sys.exit(1)

import os, time, subprocess, csv, getpass
from lib.db import get_stage2_jobids

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

    
    for flist in get_stage2_jobids(SAMPLE,WEIGHT_PREFIX):

        jobid = str(flist['jobtag'])
        INPUT_FILE  = os.path.join(flist['inputdir'],"evweight_art_%s.root" % jobid)
        OUTPUT_FILE = os.path.join(flist['inputdir'],"arborist_evweight_art_%s.root" % jobid)

        SS  = ""
        SS += "srun -p interactive --mem=2000 --job-name arbor singularity exec %s bash -c "
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
        SS += "/usr/local/share/sw/scripts/sbnfit/arborist/arborist %s %s 1>/dev/null 2>/dev/null"
        SS += " && "
        SS += "chmod 777 %s 1>/dev/null 2>/dev/null"
        SS += "\""
        SS += " &"    
        SS = SS % (CONTAINER, 
                   OUTPUT_FILE.replace("90-days-archive",""),
                   INPUT_FILE.replace("90-days-archive",""),
                   OUTPUT_FILE.replace("90-days-archive",""))


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

