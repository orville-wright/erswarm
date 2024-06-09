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

#######################################################################################
#  SWARM headers

    url = "https://openebs-phonehome.cloud.datacore.com"

    token = "8501d22487a85a5f819c21f0b5152f60"

    token = {"token": "f9adcb6a724743fe3dd84484df69cacb" }   # # token for access - expires 2027-08-28

    swarm_headers = {
                    "domain": "openebs-phonehome.cloud.datacore.com",
                    "CAStor-Application": "Phone Home Server*",
                    "stype": "unnamed",
                    "format": "json",
                    "stype": "all",
                    "size": "200",
                    "fields": "X-Phonehome-Meta-Castor-Cluster-Id,name,tmBorn",
                    "sort": "X-Phonehome-Meta-Castor-Cluster-Id" }

    def __init__(self, yti, global_args):
        cmi_debug = __name__+"::"+self.__init__.__name__
        self.args = global_args
        logging.info( f'%s - Instantiate.#{yti}' % cmi_debug )
        # init empty DataFrame with present colum names
        #self.tg_df0 = pd.DataFrame(columns=[ 'Row', 'Symbol', 'Co_name', 'Cur_price', 'Prc_change', 'Pct_change', 'Mkt_cap', 'M_B', 'Time'] )
        self.inst_uid = yti
        ##self.js_session = HTMLSession()                        # init JAVAScript processor early
        self.session = requests.Session()
        self.uniqueClusters = set()
        cluster_count = dict()

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
        process some extracted nucket data
        Must be given a LIST that has JDON encoded data fields
        Will identify....
              the unique clsuter ids
              count how many ping day/entries each cluster has sent to the swarm busket
        """

        cmi_debug = __name__+"::"+"_INST.#"+str(self.inst_uid)
        logging.info('%s - Processing a list of extracted data' % cmi_debug )
        self.jdata = json.loads(jlist)

        print ( f"Extracted elemts from bucket: %s" % len(self.jdata) )

        x = 1
        c = 1

        for i in self.jdata:
            cluster_id = i['x_phonehome_meta_castor_cluster_id']
            hashed_cid = hashlib.md5(cluster_id.encode())
            hashed_cluster = hashed_cid.hexdigest()
            print ( f"Cluster: {x} : {cluster_id} : {hashed_cluster}" )
            x += 1

            self.uniqueClusters.add(hashed_cid.hexdigest())

            if hashed_cluster in self.cluster_count:
                print ( f"Found cluster in count dict - incrementing count..." )
                y = self.cluster_count[hashed_cluster]
                y += 1
                self.cluster_count[hashed_cluster] = y
            else:
                print ( f"Adding cluster to count dic..." )
                self.cluster_count[hashed_cluster] = int(1)

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

        d = 1 
        for j in self.cluster_count:
            print ( f"Cluster #{d:3}  : Unique cluster: {j} - Ping enteries: {self.cluster_count[j]}" )
            d += 1
        

        return