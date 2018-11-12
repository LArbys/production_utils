import os,sys,gc
import pandas as pd

BASE_PATH = os.path.realpath(__file__)
BASE_PATH = os.path.dirname(BASE_PATH)
sys.path.insert(0,BASE_PATH)

def concat_pkls(flist_v,OUT_NAME="out"):
    df_v = []
    row_ctr = 0

    for ix,f in enumerate(flist_v):
        try:
            df = pd.read_pickle(f)
        except:
            print "\033[91m" + "ERROR @file=%s" % os.path.basename(f) + "\033[0m"
            continue
                    
        df_v.append(df)

        row_ctr += int(df_v[-1].index.size)
        print "\r(%02d/%02d)... read %d rows" % (ix,len(flist_v),row_ctr),
        sys.stdout.flush()
        
    if len(df_v)==0: 
        print "nothing to see..."
        return False
    
    df = pd.concat(df_v,ignore_index=True)
    print "...concat"
    SS = OUT_NAME + ".pkl"
    df.to_pickle(SS)
    dsk  = os.path.getsize(SS)
    dsk /= (1024.0 * 1024.0)
    print "...saved {:.02f} MB".format(dsk)
    del df
    gc.collect()
    print "...reaped"
    return True

def main(argv):

    if len(argv) < 3:
        print 
        print "......................"
        print "OUT_PREFIX = str(sys.argv[1])"
        print "PKLS_v     = list(sys.argv[2:])"
        print "......................"
        print 
        sys.exit(1)

    OUT_PREFIX = str(sys.argv[1])
    PKLS_v = list(sys.argv[2:])
    PKLS_v = [str(f) for f in PKLS_v]

    concat_pkls(PKLS_v,OUT_PREFIX)

    return

if __name__ == "__main__":
    main(sys.argv)
    sys.exit(1)
