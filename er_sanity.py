#!/usr/bin/python3

import urllib
import urllib.request
import pandas as pd
import numpy as np
import re
import logging
import argparse
import time
import json
from urllib.parse import urlparse
from rich import print

# logging setup
logging.basicConfig(level=logging.INFO)

# my private classes & methods
from bucket_retriever import retriever

# Globals

work_inst = 0
global args
args = {}

global parser
parser = argparse.ArgumentParser(description="Ed's Mayadata Metrics Sanity checker")
parser.add_argument('-s','--show', help='show some bucket data', action='store_true', dest='bool_show', required=False, default=False)
parser.add_argument('-v','--verbose', help='verbose error logging', action='store_true', dest='bool_verbose', required=False, default=False)
parser.add_argument('-x','--xray', help='dump Xray debug data structures', action='store_true', dest='bool_xray', required=False, default=False)


#################### 0 - MAIN ###########################

def main():
    cmi_debug = "er_sanity::"+__name__+"::main()"
    global args
    args = vars(parser.parse_args())        # args as a dict []
    print ( " " )
    print ( "########## Initalizing ##########" )
    print ( " " )
    print ( "CMDLine args:", parser.parse_args() )
    if args['bool_verbose'] is True:        # Logging level
        print ( "Enabeling verbose info logging..." )
        logging.disable(0)                  # Log level = OFF
    else:
        logging.disable(20)                 # Log lvel = INFO

    print ( " " )


########## 1 - GET BUCKET DATA  ################
    if args['bool_show'] is True:
        print ( "========== Show Bucket data / a few rows ===============================" )

        show_data = retriever(1, args)      # instantiate class
        data_payload = show_data.do_simple_get()
        # print ( f"{data_payload}" )t

        print ( " " )

        show_data.j_clusters(data_payload)

        #show_data.print_unique_clusters()

        show_data.print_cluster_count()

        pd.set_option('display.max_rows', None)
        print ( f"{show_data.global_df0}" )
        g_df = pd.DataFrame(show_data.global_df0.sort_values(by=['pingcount'], ascending=True).groupby(['pingcount'])['cluster'].count() )
        #g_df.loc['Averages'] = g_df.mean()

        print ( f"============= grouping ===================" )
        print ( f"{g_df}" )

#################### exec ######################

if __name__ == '__main__':
    main()
