import sys

print "-----------------------"
print "| DL RUN1 EXTRACTOR   |"
print "-----------------------"

if len(sys.argv) != 5:
    print ""
    print "RSE_CSV = str(sys.argv[1])"
    print "PREFIX  = str(sys.argv[2])"
    print "STAGE   = str(sys.argv[3])"
    print "OUTPUT  = str(sys.argv[4])"
    print ""
    sys.exit(1)

import os, time, subprocess, csv, getpass

def exec_system(input_):
    return subprocess.Popen(input_,stdout=subprocess.PIPE).communicate()[0].split("\n")[:-1]

def njobs():
    ret = exec_system(["squeue","-u",getpass.getuser()])
    ret = [r for r in ret if "interacti" in r]
    return int(len(ret))

def main() :
    
    SAMPLE  = "run1numu"
    #SAMPLE  = "mcc8v6_bnb5e19"
    RSE_CSV = str(sys.argv[1])
    PREFIX  = str(sys.argv[2])
    STAGE   = str(sys.argv[3])
    OUTPUT  = str(sys.argv[4])
    
    OUTPUT = os.path.join(os.getcwd(),OUTPUT)
    
    DB_DIR = "/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db"
    
    STAGE1_DIR = os.path.join(DB_DIR,SAMPLE,"stage1")
    STAGE2_DIR = os.path.join(DB_DIR,SAMPLE,"stage2","test11")
        
    PKL_FILE = os.path.join(DB_DIR,"rse","samples",SAMPLE,"comb.pkl")

    RSE_t = []
    with open(RSE_CSV,'rb') as csvfile:
        reader = csv.reader(csvfile,delimiter=",")
        for row in reader:
            RSE_t.append((row[0],row[1],row[2]))

    #
    # execute
    #
    CONTAINER  = "/cluster/tufts/wongjiradlab/vgenty/vertex/vertex_02212018_p01/image/"
    CONTAINER += "singularity-dllee-unified-02212018_revert.img"

    SS_v = []
    for RSE in RSE_t:
        print "@rse=%s" % str(RSE)
        run,subrun,event = RSE
        SS = ""
        SS += "module load singularity" 
        SS += " && "
        SS += "srun -p interactive --mem=2000 --job-name ext_ singularity exec %s bash -c "
        SS += "\"source /usr/local/bin/thisroot.sh 1>/dev/null 2>/dev/null"
        SS += " && "
        SS += "cd /usr/local/share/dllee_unified/ 1>/dev/null 2>/dev/null"
        SS += " && " 
        SS += "source configure.sh 1>/dev/null 2>/dev/null"
        SS += " && "
        SS += "python /usr/local/share/dllee_unified/LArCV/app/LArOpenCVHandle/ana/entry_copy/single_copy.py %s %s %s %s %s %s %s %s %s\""
    
        SS = SS % (CONTAINER, 
                   PKL_FILE,
                   run,
                   subrun,
                   event,
                   STAGE1_DIR,
                   STAGE2_DIR,
                   PREFIX,
                   STAGE,
                   OUTPUT)

        SS_v.append(SS)

    while 1:
        if njobs() < 50:
            SS = SS_v.pop()
            SS += " &"
            os.system(SS)
            print "Exec @ interactive node njobs=%d" % njobs()
        else:
            time.sleep(1)
            print "...sleep..."
        if len(SS_v) == 0: break

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

