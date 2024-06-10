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
parser.add_argument('-s','--scan', help='scan swarm bucket data', action='store_true', dest='bool_scan', required=False, default=False)
parser.add_argument('-t','--token', help='Swarm bucket token to use', action='store', dest='swarm_token', required=True, default=False)
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

    if args['swarm_token'] is not False:
        use_token = args['swarm_token']
        print ( f"using Bucket token: {use_token}" )

    print ( " " )


########## 1 - GET BUCKET DATA  ################
    if args['bool_scan'] is True:
        print ( "======================= Scan Swarm Bucket data  ===============================" )

        scan_data = retriever(1, use_token, args)      # instantiate class

        data_payload = scan_data.do_simple_get()
        print ( " " )
        scan_data.j_clusters(data_payload)

        scan_data.gen_cluster_count(0)      # 0 = be silent, dont display progress / 1 = display progress

        pd.set_option('display.max_rows', None)
        print ( f"{scan_data.global_df0}" )
        g_df = pd.DataFrame(scan_data.global_df0.sort_values(by=['days_pinged'], ascending=True).groupby(['days_pinged'])['cluster'].count() )
        #g_df.loc['Averages'] = g_df.mean()

        print ( f"============= grouping ===================" )
        print ( f"{g_df}" )

#################### exec ######################

if __name__ == '__main__':
    main()
