import json
from flask import Flask
from flask import request
from flask_cors import CORS, cross_origin
import mysql.connector
import datetime
from dateutil.tz import *
from dateutil.relativedelta import *
import re
import logging
import os
import sys
from mysql.connector import errorcode
from calculate import ecs_getClusterArn, get_datetime_start_end, cost_of_service, get, getConnection



app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


cpu2mem_weight = 0.5
pricing_dict = {}
global region_table
region_table = {}


@app.route("/compute/price", methods=["GET"])
@cross_origin()
def compareServices_():
    print("attempting to commpute price")
    
    accessToken = request.args.get("token")
    region = request.args.get('region_id')
    service = request.args.get("service_id")
    clustername = request.args.get("cluster")
    filterValue = request.args.get("filter_value")
    filterFormat = request.args.get("filter_format")

    metered_results = True if (filterFormat and filterValue) else False

    #logging.basicConfig(level=logging.DEBUG)

    print(accessToken)
    print(region)
    print(service)
    print(clustername)
    print(filterValue)
    print(filterFormat)


    month = None
    days = None
    hours = None

    if(filterFormat=="H"):
        hours = filterValue
    elif(filterFormat=="M"):
        month = filterValue
    elif(filterFormat=="D"):
        days = filterValue

    response = {}

    cluster = ecs_getClusterArn(region, clustername)
    if not cluster:
        logging.error("Cluster : %s Missing", clustername)
        response['status'] =  "error"
        response['message']  = "invalid cluster_id"
        return response

    try:
        with open('region_table.json') as f:
            region_table = json.load(f)
            if region not in region_table.keys():
                raise
    except Exception as e:
        print(e)
        print("Unexpected error: Unable to read region_table.json or region (%s) not found" % (region))
        response['status'] =  "error"
        response['message']  = "internal server error, could read .json file"
        return response
        sys.exit(1)

    now = datetime.datetime.now(tz=tzutc())
    database = getConnection()
    

    tasks = get(region, cluster, service, database, region_table)

    if metered_results:
        (meter_start, meter_end) = get_datetime_start_end(now, month, days, hours)
        print("meter_"+str(meter_start))
        print("_meter"+str(meter_end))
        if(meter_start is None or meter_end is None):
            response['status'] =  "error"
            response['message']  = "invalid filter format provided" # -M ->  MM/YY, -D -> integer value, -H -> integer value hours
            return response
        (fg_cpu, fg_mem, ec2_mem, ec2_cpu) = cost_of_service(tasks, meter_start, meter_end, now)
    else: 
        (fg_cpu, fg_mem, ec2_mem, ec2_cpu) = cost_of_service(tasks, 0, 0, now)


    


    logging.debug("Main: fg_cpu=%f, fg_mem=%f, ec2_mem=%f, ec2_cpu=%f", fg_cpu, fg_mem, ec2_mem, ec2_cpu)


    if not (fg_cpu or fg_mem or ec2_mem or ec2_cpu):
        response['status'] =  "error"
        response['message']  = "service not running during specified time duration"
    else:
        response['status'] =  "success"

    if ec2_mem or ec2_cpu:
        response['total_cost'] = (ec2_mem+ec2_cpu)
        response['cpu_cost'] = (ec2_cpu)
        response['memory_cost'] = (ec2_mem)

    if fg_cpu or fg_mem:
        response['total_cost'] = (fg_mem+fg_cpu)
        response['cpu_cost'] = (fg_cpu)
        response['memory_cost'] = (fg_mem)

    database.close()
    return response







if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True,threaded=True)