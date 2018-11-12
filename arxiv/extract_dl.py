import sys

print "-----------------------"
print "| DL DATA EXTRACTOR   |"
print "-----------------------"

if len(sys.argv) != 6:
    print ""
    print "SAMPLE        = str(sys.argv[1]) -- https://goo.gl/56E69r"
    print "VERTEX_RUNTAG = str(sys.argv[2])"
    print "STP_LL_RUNTAG = str(sys.argv[3])"
    print "RSE_CSV       = str(sys.argv[4])"
    print "OUTPUT        = str(sys.argv[5])"
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
    SAMPLE        = str(sys.argv[1])
    VERTEX_RUNTAG = str(sys.argv[2])
    STP_LL_RUNTAG = str(sys.argv[3])
    RSE_CSV       = str(sys.argv[4])
    OUTPUT        = str(sys.argv[5])
    
    OUTPUT = os.path.join(os.getcwd(),OUTPUT)
    
    DB_DIR = "/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db"
    
    STAGE1_DIR = os.path.join(DB_DIR,SAMPLE,"stage1")
    
    IS_MC = 0
    if "cocktail" in SAMPLE:
        STAGE1_DIR = os.path.join(DB_DIR,SAMPLE)
        IS_MC = 1

    if "corsika" in SAMPLE:
        STAGE1_DIR = os.path.join(DB_DIR,SAMPLE)

    STAGE2_DIR = os.path.join(DB_DIR,SAMPLE,"stage2")

    VERTEX_DIR = os.path.join(STAGE2_DIR,VERTEX_RUNTAG)
    STP_LL_DIR = os.path.join(STAGE2_DIR,STP_LL_RUNTAG)
    
    PKL_FILE = os.path.join(DB_DIR,"rse","samples",SAMPLE,"comb.pkl")
    
    if os.path.exists(PKL_FILE) == False:
        print "NO rse file is available for this sample"
        print "        please contact vic              "
        print "         skype:@vicgenty                "
        sys.exit(1)


    RSE_t = []
    with open(RSE_CSV,'rb') as csvfile:
        reader = csv.reader(csvfile,delimiter=",")
        for row in reader:
            RSE_t.append((row[0],row[1],row[2]))

    #
    # execute
    #
    CONTAINER  = "/cluster/kappa/90-days-archive/wongjiradlab/vgenty/vertex/vertex_05022018_p00/image/"
    CONTAINER += "singularity-dllee-unified-05022018_revert.img"

    SS_v = []
    for RSE in RSE_t:
        print "@rse=%s" % str(RSE)
        run,subrun,event = RSE
        SS = ""
        SS += "module load singularity" 
        SS += " && "
        SS += "srun -p interactive --mem=3000 --job-name ext_ singularity exec %s bash -c "
        SS += "\"source /usr/local/bin/thisroot.sh 1>/dev/null 2>/dev/null"
        SS += " && "
        SS += "cd /usr/local/share/dllee_unified/ 1>/dev/null 2>/dev/null"
        SS += " && " 
        SS += "source configure.sh 1>/dev/null 2>/dev/null"
        SS += " && "
        SS += "python /usr/local/share/dllee_unified/LArCV/app/LArOpenCVHandle/ana/entry_copy/rse_copy.py %s %s %s %s %s %s %s %s %s\""
    
        SS = SS % (CONTAINER, 
                   PKL_FILE.replace("90-days-archive",""),
                   run,
                   subrun,
                   event,
                   STAGE1_DIR.replace("90-days-archive",""),
                   VERTEX_DIR.replace("90-days-archive",""),
                   STP_LL_DIR.replace("90-days-archive",""),
                   IS_MC,
                   OUTPUT.replace("90-days-archive",""))

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
    sys.exit(0)

