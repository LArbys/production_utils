import sys, os
import psycopg2

PUB_PSQL_ADMIN_HOST="nudot.lns.mit.edu"
PUB_PSQL_ADMIN_USER="tufts-pubs"
PUB_PSQL_ADMIN_ROLE=""
PUB_PSQL_ADMIN_DB="procdb"
PUB_PSQL_ADMIN_PASS=""
PUB_PSQL_ADMIN_CONN_NTRY="10"
PUB_PSQL_ADMIN_CONN_SLEEP="10"

PARAMS = {
  'dbname': PUB_PSQL_ADMIN_DB,
  'user': PUB_PSQL_ADMIN_USER,
  'password': PUB_PSQL_ADMIN_PASS,
  'host': PUB_PSQL_ADMIN_HOST
  }

print PARAMS
CONN = psycopg2.connect(**PARAMS)
CUR = CONN.cursor()                             

def cast_run_subrun(run,subrun,input_dir):


    runmod100    = run%100
    rundiv100    = run/100
    subrunmod100 = subrun%100
    subrundiv100 = subrun/100
    
    jobtag      = 10000*run + subrun
    inputdbdir  = os.path.join(input_dir,"%03d/%02d/%03d/%02d/"%(rundiv100,runmod100,subrundiv100,subrunmod100))

    return jobtag, inputdbdir

def get_stage1_flist(project,IS_MC):

    res_v = []
    
    input_format = "%s-Run%06d-SubRun%06d.root"

    if IS_MC == 0:
        input_dir = "/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db/%s/stage1/"
    else:
        input_dir = "/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db/%s/"

    input_dir = input_dir % project

    SS = '''SELECT runnumber,subrunnumber FROM %s;'''
    SS = SS % project
    print SS
    CUR.execute(SS)
    print "Fetching..."
    rse_v =  CUR.fetchall()
    print "...fetched"
    print "Exist (%d)..." % len(rse_v)
    for ix,rs in enumerate(rse_v):
        SS = "%0.1f\r" % (float(ix) / float(len(rse_v)) * float(100.0))
        print SS,
        sys.stdout.flush()

        run    = rs[0]
        subrun = rs[1]

        jobtag, inputdir = cast_run_subrun(rs[0],rs[1],input_dir)

        res = {}
        res = { 
            'reco2d' : os.path.join(inputdir,input_format%("reco2d",run,subrun)),
            'mcinfo' : os.path.join(inputdir,input_format%("mcinfo",run,subrun)),
            'opreco' : os.path.join(inputdir,input_format%("opreco",run,subrun)),
            'supera' : os.path.join(inputdir,input_format%("supera",run,subrun)),
            
            'ssnetout-larcv'   : os.path.join(inputdir,input_format%("ssnetout-larcv",run,subrun)),
            'ssnetserverout-larcv' : os.path.join(inputdir,input_format%("ssnetserverout-larcv",run,subrun)),
            'ssnetserveroutv2-larcv' : os.path.join(inputdir,input_format%("ssnetserveroutv2-larcv",run,subrun)),
            
            'taggerout-larcv'   : os.path.join(inputdir,input_format%("taggerout-larcv",run,subrun)),
            'taggeroutv2-larcv' : os.path.join(inputdir,input_format%("taggeroutv2-larcv",run,subrun)),
            
            'taggerout-larlite'   : os.path.join(inputdir,input_format%("taggerout-larlite",run,subrun)),
            'taggeroutv2-larlite' : os.path.join(inputdir,input_format%("taggeroutv2-larlite",run,subrun)),

            'run' : run,
            'subrun' : subrun,
            'jobtag' : jobtag
            }
        
        res_v.append(res)
        res = {}
        
    print "...exists"
    return res_v


def get_stage2_jobids(project,tag):

    res_v = []
    
    input_dir = "/cluster/kappa/90-days-archive/wongjiradlab/larbys/data/db/%s/stage2/%s/"
    input_dir = input_dir % (project,tag)

    SS = '''SELECT runnumber,subrunnumber FROM %s;'''
    SS = SS % project
    print SS
    CUR.execute(SS)
    print "Fetching..."
    rse_v =  CUR.fetchall()
    print "...fetched"
    print "Exist (%d)..." % len(rse_v)
    for ix,rs in enumerate(rse_v):
        SS = "%0.1f\r" % (float(ix) / float(len(rse_v)) * float(100.0))
        print SS,
        sys.stdout.flush()

        run    = rs[0]
        subrun = rs[1]

        jobtag, inputdir = cast_run_subrun(rs[0],rs[1],input_dir)

        res = {}
        res = { 
            'inputdir' : inputdir,
            'run' : run,
            'subrun' : subrun,
            'jobtag' : jobtag
            }
        
        res_v.append(res)
        res = {}
        
    print "...exists"
    return res_v
