import sys

print "-----------------------"
print "| DL sbnfit inputter  |"
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

SAMPLE         = str(sys.argv[1])
WEIGHT_PREFIX  = str(sys.argv[2])

DB_DIR = "/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db"

#
# execute
#
CONTAINER  = "/cluster/kappa/90-days-archive/wongjiradlab/vgenty/images/"
CONTAINER += "evweight_uboonecode_11092018.img"

root_input_file_v = []
mcflux_input_file_v = []
andy_input_file_v = []

inputdir = ""
for flist in get_stage2_jobids(SAMPLE,WEIGHT_PREFIX):
    
    jobid    = str(flist['jobtag'])
    inputdir = str(flist['inputdir'])
    ROOT_INPUT_FILE   = os.path.join(inputdir,"arborist_evweight_art_%s.root" % jobid)
    ANDY_INPUT_FILE   = os.path.join(inputdir,"andy_out_%s.root" % jobid)
    MCFLUX_INPUT_FILE = os.path.join(inputdir,"mcflux_dump_%s.txt" % jobid)
    
    root_input_file_v.append(ROOT_INPUT_FILE.replace("90-days-archive",""))
    andy_input_file_v.append(ANDY_INPUT_FILE.replace("90-days-archive",""))
    mcflux_input_file_v.append(MCFLUX_INPUT_FILE)


outdir = str(inputdir)
outdir = outdir.split(WEIGHT_PREFIX)[0]
outdir = os.path.join(outdir,WEIGHT_PREFIX,"sbnfit")
SS = "mkdir -p %s" % outdir
os.system(SS)

out_root_file_name   = os.path.join(outdir,"arborist_evweight_art.root")
out_andy_file_name   = os.path.join(outdir,"andy_out.root")
out_mcflux_file_name = os.path.join(outdir,"mcflux_dump.txt")

arborist_file = os.path.join(outdir,"arborist_file_list.txt")
with open(arborist_file,"w") as f:
    root_file_list = "\n".join(root_input_file_v)
    f.write(root_file_list)

andy_file = os.path.join(outdir,"andy_file_list.txt")
with open(andy_file,"w") as f:
    andy_file_list = "\n".join(andy_input_file_v)
    f.write(andy_file_list)

mcflux_file = os.path.join(outdir,"mcflux_file_list.txt")
with open(mcflux_file,"w") as f:
    mcflux_file_list = "\n".join(mcflux_input_file_v)
    f.write(mcflux_file_list)

arborist_file_std_out = os.path.join(outdir,"stdout_arborist.txt")
SS = "rm -rf %s" % arborist_file_std_out
print SS
os.system(SS)

arborist_file_std_err = os.path.join(outdir,"stderr_arborist.txt")
SS = "rm -rf %s" % arborist_file_std_err
print SS
os.system(SS)

andy_file_std_out = os.path.join(outdir,"stdout_andy.txt")
SS = "rm -rf %s" % andy_file_std_out
print SS
os.system(SS)

andy_file_std_err = os.path.join(outdir,"stderr_andy.txt")
SS = "rm -rf %s" % andy_file_std_err
print SS
os.system(SS)

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
SS += "cd /usr/local/share/sw/ 1>/dev/null 2>/dev/null"
SS += " && "
SS += "source larlite/config/setup.sh 1>/dev/null 2>/dev/null"
SS += " && "
SS += "hadd -f %s @%s 1>%s 2>%s"
SS += " && "
SS += "hadd -f %s @%s 1>%s 2>%s"
SS += "\""
SS = SS % (CONTAINER,
           out_root_file_name.replace("90-days-archive",""),
           arborist_file.replace("90-days-archive",""),
           arborist_file_std_out.replace("90-days-archive",""),
           arborist_file_std_err.replace("90-days-archive",""),
           out_andy_file_name.replace("90-days-archive",""),
           andy_file.replace("90-days-archive",""),
           andy_file_std_out.replace("90-days-archive",""),
           andy_file_std_err.replace("90-days-archive",""))

print SS
os.system(SS)

mcflux_file = os.path.join(outdir,"mcflux_dump.txt")
with open(mcflux_file,'w') as f:
    for mcflux_input_file in mcflux_input_file_v:
        with open(mcflux_input_file,'r') as g:
            data = g.read()
            f.write(data)

sys.exit(1)
