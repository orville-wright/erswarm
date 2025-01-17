#!/usr/bin/python3
import requests
from requests import Request, Session
#from requests_html import HTMLSession
import pandas as pd
import numpy as np
import re
import logging
import argparse
import time
import json
import hashlib
from rich import print

# logging setup
logging.basicConfig(level=logging.INFO)

#####################################################

class retriever:
    """Class to extract data from Swarm bucket"""

    # global accessors
    tg_df0 = ""          # DataFrame - Full list of top gainers
    rows_extr = 0        # number of rows of data extracted
    inst_uid = 0
    args = []            # class dict to hold global args being passed in from main() methods
    cycle = 0            # class thread loop counter
    uniqueClusters = 0
    cluster_count = {}
    global_df0 = ""     # Pandas DataFram for analytics, statistics work
    token = {}

#######################################################################################
#  SWARM headers

    url = "https://openebs-phonehome.cloud.datacore.com"


    swarm_headers = {
                    "domain": "openebs-phonehome.cloud.datacore.com",
                    "CAStor-Application": "Phone Home Server*",
                    "stype": "unnamed",
                    "format": "json",
                    "stype": "all",
                    "size": "3000",
                    "fields": "X-Phonehome-Meta-Castor-Cluster-Id,name,tmBorn",
                    "sort": "X-Phonehome-Meta-Castor-Cluster-Id" }

    def __init__(self, yti, t, global_args):
        cmi_debug = __name__+"::"+self.__init__.__name__
        self.args = global_args
        logging.info( f'%s - Instantiate.#{yti}' % cmi_debug )
        # init empty Class Global DataFrame
        self.global_df0 = pd.DataFrame()                   # new df, but is NULLed
        self.inst_uid = yti
        self.session = requests.Session()
        self.uniqueClusters = set()
        self.cluster_count = dict()
        self.token = {"token": t }

        return

######################################################################
# method 1
    def do_simple_get(self):
        """
        basic get buck data
        """

        cmi_debug = __name__+"::"+"_INST.#"+str(self.inst_uid)
        logging.info('%s - request get() on base url {self.url}' % cmi_debug )

        #with self.js_session.get('https://openebs-phonehome.cloud.datacore.com', stream=True, headers=self.swarm_headers, timeout=5 ) as self.js_resp0:

        r = self.session.get(self.url, params=self.swarm_headers, cookies=self.token, timeout=600)

        logging.info('%s - Request get() done' % cmi_debug )

        logging.info('%s - close url handle' % cmi_debug )
        r.close()

        # if the get() succeds, the response handle is automatically saved in Class Global accessor -> self.js_resp0

        logging.info( f"%s - request STATUS_CODE: {r.status_code}" % cmi_debug )

        return r.text


######################################################################
# method 2
    def j_clusters(self, jlist ):
        """
        process some extracted bucket data
        Must be given a LIST that has JSON encoded data fields
        Will identify....
              1. the unique clsuter ids
              2. count how many ping day/entries each cluster has sent to the swarm busket
        """

        cmi_debug = __name__+"::"+"_INST.#"+str(self.inst_uid)
        logging.info('%s - Processing a list of extracted data' % cmi_debug )
        self.jdata = json.loads(jlist)

        print ( f"Extracted elements from bucket: %s" % len(self.jdata) )

        x = 1
        c = 1
        z = 0

        for i in self.jdata:
            cluster_id = i['x_phonehome_meta_castor_cluster_id']    # k8s cluster id is a veerrrry long numbers
            hashed_cid = hashlib.md5(cluster_id.encode())           # use crypto hash to make a shorter but unique number from k8s cluster id
            hashed_cluster = hashed_cid.hexdigest()                 # generate a short unique cluster id from the long k8s cluster id
            x += 1
            z += 1

            self.uniqueClusters.add(hashed_cid.hexdigest())

            if hashed_cluster in self.cluster_count:          # does this cluster id allready exist in our tracking DICT?
                y = self.cluster_count[hashed_cluster]        # get the num of times we've seen iot so far
                y += 1                                        # incr count
                self.cluster_count[hashed_cluster] = y        # update new count
            else:
                self.cluster_count[hashed_cluster] = int(1)   # add new cluster id into tracking DICT, with count = 1

        print ( f"Total objects processed: {z}" )
        return

######################################################################
# method 3
    def print_unique_clusters(self):
        """
        print the contents of the Unique Clsuters set
        """

        cmi_debug = __name__+"::"+"_INST.#"+str(self.inst_uid)
        logging.info('%s - Print unique clusters list' % cmi_debug )
       
        c = 1 
        for i in self.uniqueClusters:
            print ( f"Cluster #{c:3}  : Unique cluster: {i}" )
            c += 1

        return

######################################################################
# method 4
    def gen_cluster_count(self, prt_status):
        """
        Generate the count of unique clusters
        """

        cmi_debug = __name__+"::"+"_INST.#"+str(self.inst_uid)
        logging.info('%s - Generate cluster count' % cmi_debug )

        idx = 0
        d = 1 
        for j in self.cluster_count:
            if prt_status == 1:             # be verbose and display progress of what we're doing
                print ( f"Cluster #{d:3}  : Unique_cluster: {j} - Ping_enteries: {self.cluster_count[j]}" )

            self.build_df0(1, idx, d, j, self.cluster_count[j])
            d += 1
            idx += 1
       
        return

######################################################################
# method 5
    def build_df0(self, yti, idx, clusternum, clusterhash, pingcount):
        """
        Build-out a fully populated Pandas DataFrame containg key fields
        This will allow us to do intersting statistical/math analytics on the data using Pandas
        translations rathe than dict,list,set python code
        """

        cmi_debug = __name__+"::"+self.build_df0.__name__+".#"+str(yti)
        self.time_now = time.strftime("%H:%M:%S", time.localtime() )
        x = 0

        logging.info( f"%s - Build list for Dataframe insert: {clusternum}" % cmi_debug )        # so we can access it natively if needed, without using pandas
        self.data0 = [[ \
            clusternum, \
            clusterhash, \
            pingcount, \
            self.time_now ]]

        logging.info( f"%s - Prepare DF for new cluster entry: {clusternum}" % cmi_debug )
        # convert our list into a 1 row dataframe
        # self.df0_new_row = pd.DataFrame(self.data0, columns=[ 'Row', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time' ], index=[x] )
        self.df0_new_row = pd.DataFrame(self.data0, columns=[ 'cluster', 'Uniqueid', 'days_pinged', 'Time_now' ], index=[idx] )

        self.global_df0 = pd.concat([self.global_df0, self.df0_new_row])
        logging.info('%s - Cluster DF entry created' % cmi_debug )
        x+=1

        return 0
