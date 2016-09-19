#!/usr/bin/env python2.7
# Prototype automatic deployment of a CM cluster on a single host
# Adapted from https://github.com/cloudera/cm_api/blob/master/python/examples/cluster_set_up.py

# TODO - periodic polling on long-running commands
# to retrieve progress / subcommand information

# Usage: $0 HOST
import os
import argparse
import logging
import time
from cm_api.api_client import ApiResource
from cm_api.endpoints.clusters import create_cluster
from cm_api.endpoints.services import ApiServiceSetupInfo
from cm_api.endpoints.users import update_user

from cm_api.endpoints.types import ApiClusterTemplate
from cm_api.endpoints.cms import ClouderaManager
import json

logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)


# Parse arguments
argParser = argparse.ArgumentParser()
argParser.add_argument("action", help=" (dump | load)")
argParser.add_argument("cluster", help="cluster name ")
argParser.add_argument("-host", help="CM host (default localhost) ",default="localhost")
argParser.add_argument("-file", help="template file",default="")
argParser.add_argument("-user", help="username",default="admin")
argParser.add_argument("-passwd", help="password",default="admin")

args = argParser.parse_args()

# Configuration
hostname = args.host
action = args.action
clusterName = args.cluster
template_file = args.file or None
resource = None
if template_file in [None,""]:
    template_file="./"+clusterName.replace(" ","_")+".json"

login=args.user
passwd=args.passwd

def gen_template(clustername,template_file=None):
    resource =get_resource()
    cluster = resource.get_cluster(clustername)
    template = cluster.export()
    if os.path.exists(template_file):
        logging.info(" The preivous template is being overwitten")
    with open(template_file, 'w') as writer:
        json.dump(template.to_json_dict(), writer, indent=4, sort_keys=True)
    return


def apply_template(template_location):
    resource =get_resource()
    with open(template_location,'r') as data_file:
        data = json.load(data_file)
    template = ApiClusterTemplate(resource).from_json_dict(data, resource)
    cms = ClouderaManager(resource)
    command = cms.import_cluster_template(template)
    return


def get_resource():
    global resource,hostname,login,passwd
    if resource == None : 
        resource = resource = ApiResource(hostname, 7180, login, passwd, version=12)
    return resource

##### MAIN ###### 
if action == "dump" :
    logging.info(" Start dumping configuration from "+clusterName+" to "+template_file)
    gen_template(clusterName,template_file)
elif action == "load" :
    logging.info(" Start loading configuration into "+clusterName+" from "+template_file)
else :
    print(" Invalid action us -h for usage")


