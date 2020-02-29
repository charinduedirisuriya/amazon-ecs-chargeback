import boto3
from boto3.dynamodb.conditions import Key, Attr
import ast
import mysql.connector
from mysql.connector import errorcode
from argparse import ArgumentParser
import datetime
from dateutil.tz import *
from dateutil.relativedelta import *
import re
import sys
import logging
import json
import os

region_table = {}

cpu2mem_weight = 0.5
pricing_dict = {}


def getConnection():
  try:
    cnx = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="password",database='ecs_task_tracker'
    )
    return cnx
  except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
      print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
      print("Database does not exist")
    else:
      print(err)
    return None

def get(region, cluster, service, db, regionTable):
    serviceFilter = ""
    if(service!="*"):
        serviceFilter = " AND groupName = '"+service+"'"
    global region_table
    region_table = regionTable
    data = {'Items':[]}
    sql  = "SELECT * FROM task_definitions WHERE clusterArn = '"+cluster+"' AND region = '"+region+"' AND `group` = 'service'"  +serviceFilter
    mycursor = db.cursor()
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    for row in myresult:
        rowItem = {'taskArn':row[0], 'clusterArn':row[1], 'containerInstanceArn':row[2], 'cpu':row[3], 'group':row[4], 'groupName':row[5], 'instanceId':row[6], 'instanceType': row[7], 'launchType':row[8], 'memory':row[9], 'osType': row[10], 'region': row[11], 'runTime': row[12], 'startedAt':row[13], 'stoppedAt':row[14]}
        data['Items'].append(rowItem)
    print(data)
    return data

def ecs_getClusterArn(region, cluster):
    """ Given the ECS cluster name and the region, get the ECS ClusterARN. """
    client=boto3.client('ecs', region_name=region)
    response = client.describe_clusters(clusters=[cluster])

    logging.debug("ECS Cluster Details: %s", response)
    if len(response['clusters']) == 1:
        return (response['clusters'][0]['clusterArn'])
    else:
        return ''

def ec2_pricing(region, instance_type, tenancy, ostype):
    """
    Query AWS Pricing APIs to find cost of EC2 instance in the region.
    Given the paramters we use at input, we should get a UNIQUE result.
    TODO: In the current version, we only consider OnDemand price. If
    we start considering actual cost, we need to consider input from 
    CUR on an hourly basis.
    """
    svc_code = 'AmazonEC2'
    client = boto3.client('pricing', region_name="us-east-1")
    response = client.get_products(ServiceCode=svc_code,
        Filters = [
            {'Type' :'TERM_MATCH', 'Field':'location',          'Value':region},
            {'Type' :'TERM_MATCH', 'Field': 'servicecode',      'Value': svc_code},
            {'Type' :'TERM_MATCH', 'Field': 'preInstalledSw',   'Value': 'NA'},
            {'Type' :'TERM_MATCH', 'Field': 'tenancy',          'Value': tenancy},
            {'Type' :'TERM_MATCH', 'Field':'instanceType',      'Value':instance_type},
            {'Type' :'TERM_MATCH', 'Field': 'operatingSystem',  'Value': ostype}
        ],
        MaxResults=100
    )

    ret_list = []
    if 'PriceList' in response:
        for iter in response['PriceList']:
            ret_dict = {}
            mydict = ast.literal_eval(iter)
            ret_dict['memory'] = mydict['product']['attributes']['memory']
            ret_dict['vcpu'] = mydict['product']['attributes']['vcpu']
            ret_dict['instanceType'] = mydict['product']['attributes']['instanceType']
            ret_dict['operatingSystem'] = mydict['product']['attributes']['operatingSystem']
            ret_dict['normalizationSizeFactor'] = mydict['product']['attributes']['normalizationSizeFactor']

            mydict_terms = mydict['terms']['OnDemand'][ list( mydict['terms']['OnDemand'].keys() )[0]]
            ret_dict['unit'] = mydict_terms['priceDimensions'][list( mydict_terms['priceDimensions'].keys() )[0]]['unit']
            ret_dict['pricePerUnit'] = mydict_terms['priceDimensions'][list( mydict_terms['priceDimensions'].keys() )[0]]['pricePerUnit']
            ret_list.append(ret_dict)
    
    ec2_cpu  = float( ret_list[0]['vcpu'] )
    ec2_mem  = float( re.findall("[+-]?\d+\.?\d*", ret_list[0]['memory'])[0] )
    ec2_cost = float( ret_list[0]['pricePerUnit']['USD'] )
    return(ec2_cpu, ec2_mem, ec2_cost)

def ecs_pricing(region):
    """
    Get Fargate Pricing in the region.
    """
    svc_code = 'AmazonECS'
    client = boto3.client('pricing', region_name="us-east-1")
    response = client.get_products(ServiceCode=svc_code, 
        Filters = [
            {'Type' :'TERM_MATCH', 'Field':'location',          'Value':region},
            {'Type' :'TERM_MATCH', 'Field': 'servicecode',      'Value': svc_code},
        ],
        MaxResults=100
    )

    cpu_cost = 0.0
    mem_cost = 0.0

    if 'PriceList' in response:
        for iter in response['PriceList']:
            mydict = ast.literal_eval(iter)
            mydict_terms = mydict['terms']['OnDemand'][list( mydict['terms']['OnDemand'].keys() )[0]]
            mydict_price_dim = mydict_terms['priceDimensions'][list( mydict_terms['priceDimensions'].keys() )[0]]
            if mydict_price_dim['description'].find('CPU') > -1:
                cpu_cost = mydict_price_dim['pricePerUnit']['USD']
            if mydict_price_dim['description'].find('Memory') > -1:
                mem_cost = mydict_price_dim['pricePerUnit']['USD']

    return(cpu_cost, mem_cost)

def get_datetime_start_end(now, month, days, hours):

    logging.debug('In get_datetime_start_end(). month = %s, days = %s, hours = %s', month, days, hours)
    meter_end = now

    if month:
        # Will accept MM/YY and MM/YYYY format as input.
        regex = r"(?<![/\d])(?:0\d|[1][012])/(?:19|20)?\d{2}(?![/\d])"
        r = re.match(regex, month)
        if not r:
            print("Month provided doesn't look valid: %s" % (month))
            return (None, None)
        [m,y] = r.group().split('/')
        iy = 2000 + int(y) if int(y) <= 99 else int(y)
        im = int(m)

        meter_start = datetime.datetime(iy, im, 1, 0, 0, 0, 0, tzinfo=tzutc())
        meter_end = meter_start + relativedelta(months=1)

    if days:
        # Last N days = datetime(now) - timedelta (days = N)
        # Last N days could also be last N compelted days.
        # We use the former approach.
        if not days.isdigit():
            print("Duration provided is not a integer: %s" % (days))
            return (None, None)
        meter_start = meter_end - datetime.timedelta(days = int(days))
    if hours:
        if not hours.isdigit():
            print("Duration provided is not a integer" % (hours))
            return (None, None)
        meter_start = meter_end - datetime.timedelta(hours = int(hours))

    return (meter_start, meter_end)

def duration(startedAt, stoppedAt, startMeter, stopMeter, runTime, now):
    """
    Get the duration for which the task's cost needs to be calculated.
    This will vary depending on the CLI's input parameter (task lifetime,
    particular month, last N days etc.) and how long the task has run.
    """
    mRunTime = 0.0
    try:
        task_start = datetime.datetime.strptime(startedAt, '%Y-%m-%dT%H:%M:%S.%fZ')
        task_start = task_start.replace(tzinfo=datetime.timezone.utc)

        if (stoppedAt == 'STILL-RUNNING'):
            task_stop = now
        else:
            task_stop = datetime.datetime.strptime(stoppedAt, '%Y-%m-%dT%H:%M:%S.%fZ')
            task_stop = task_stop.replace(tzinfo=datetime.timezone.utc)

        # Return the complete task lifetime in seconds if metering duration is not provided at input.
        if not startMeter or not stopMeter:
            mRunTime = round ( (task_stop - task_start).total_seconds() )
            logging.debug('In duration (task lifetime): mRunTime=%f',  mRunTime)
            return(mRunTime)

        # Task runtime:              |------------|
        # Metering duration: |----|     or            |----|
        if (task_start >= stopMeter) or (task_stop <= startMeter): 
            mRunTime = 0.0
            logging.debug('In duration (meter duration different OOB): mRunTime=%f',  mRunTime)
            return(mRunTime)

        # Remaining scenarios:
        #
        # Task runtime:                |-------------|
        # Metering duration:   |----------|  or   |------|
        # Calculated duration:         |--|  or   |--|
        #
        # Task runtime:                |-------------|
        # Metering duration:              |-------|
        # Calculated duration:            |-------|
        #
        # Task runtime:                |-------------|
        # Metering duration:   |-------------------------|
        # Calculated duration:         |-------------|
        #

        calc_start = startMeter if (startMeter >= task_start) else task_start
        calc_stop = task_stop if (stopMeter >= task_stop) else stopMeter

        mRunTime = round ( (calc_stop - calc_start).total_seconds() )
        logging.debug('In duration(), mRunTime = %f', mRunTime)
        return(mRunTime)
    except Exception as e:
        print(e)

def ec2_cpu2mem_weights(mem, cpu):
    # Depending on the type of instance, we can make split cost beteen CPU and memory
    # disproportionately.
    global cpu2mem_weight
    return (cpu2mem_weight)

def cost_of_ec2task(region, cpu, memory, ostype, instanceType, runTime):
    """
    Get Cost in USD to run a ECS task where launchMode==EC2.
    The AWS Pricing API returns all costs in hours. runTime is in seconds.
    """
    global pricing_dict

    pricing_key = '_'.join(['ec2',region, instanceType, ostype]) 
    if pricing_key not in pricing_dict:
        # Workaround for DUBLIN, Shared Tenancy and Linux
        (ec2_cpu, ec2_mem, ec2_cost) = ec2_pricing(region_table[region], instanceType, 'Shared', 'Linux')
        pricing_dict[pricing_key]={}
        pricing_dict[pricing_key]['cpu'] = ec2_cpu      # Number of CPUs on the EC2 instance
        pricing_dict[pricing_key]['memory'] = ec2_mem   # GiB of memory on the EC2 instance
        pricing_dict[pricing_key]['cost'] = ec2_cost    # Cost of EC2 instance (On-demand)

    # Corner case: When no CPU is assigned to a ECS Task, cpushares = 0
    # Workaround: Assume a minimum cpushare, say 128 or 256 (0.25 vcpu is the minimum on Fargate).
    if cpu == '0':
        cpu = '128'

    # Split EC2 cost bewtween memory and weights
    ec2_cpu2mem = ec2_cpu2mem_weights(pricing_dict[pricing_key]['memory'], pricing_dict[pricing_key]['cpu'])
    cpu_charges = ( (float(cpu)) / 1024.0 / pricing_dict[pricing_key]['cpu']) * ( float(pricing_dict[pricing_key]['cost']) * ec2_cpu2mem ) * (runTime/60.0/60.0)
    mem_charges = ( (float(memory)) / 1024.0 / pricing_dict[pricing_key]['memory'] ) * ( float(pricing_dict[pricing_key]['cost']) * (1.0 - ec2_cpu2mem) ) * (runTime/60.0/60.0)

    logging.debug('In cost_of_ec2task: mem_charges=%f, cpu_charges=%f',  mem_charges, cpu_charges)
    return(mem_charges, cpu_charges)

def cost_of_fgtask(region, cpu, memory, ostype, runTime):
    global pricing_dict
    global region_table

    pricing_key = 'fargate_' + region
    if pricing_key not in pricing_dict:
        # First time. Updating Dictionary
        # Workarond - for DUBLIN (cpu_cost, mem_cost) = ecs_pricing(region)
        (cpu_cost, mem_cost) = ecs_pricing(region_table[region])
        pricing_dict[pricing_key]={}
        pricing_dict[pricing_key]['cpu'] = cpu_cost
        pricing_dict[pricing_key]['memory'] = mem_cost

    mem_charges = ( (float(memory)) / 1024.0 ) * float(pricing_dict[pricing_key]['memory']) * (runTime/60.0/60.0)
    cpu_charges = ( (float(cpu)) / 1024.0 )    * float(pricing_dict[pricing_key]['cpu'])    * (runTime/60.0/60.0)

    logging.debug('In cost_of_fgtask: mem_charges=%f, cpu_charges=%f',  mem_charges, cpu_charges)
    return(mem_charges, cpu_charges)

def cost_of_service(tasks, meter_start, meter_end, now):
    fargate_service_cpu_cost = 0.0
    fargate_service_mem_cost = 0.0
    ec2_service_cpu_cost = 0.0
    ec2_service_mem_cost = 0.0

    if 'Items' in tasks:
        for task in tasks['Items']:
            try:
                runTime = duration(task['startedAt'], task['stoppedAt'], meter_start, meter_end, float(task['runTime']), now)
                logging.debug("In cost_of_service: runTime = %f seconds", runTime)
                if task['launchType'] == 'FARGATE':
                    fargate_mem_charges,fargate_cpu_charges = cost_of_fgtask(task['region'], task['cpu'], task['memory'], task['osType'], runTime)
                    fargate_service_mem_cost += fargate_mem_charges
                    fargate_service_cpu_cost += fargate_cpu_charges
                else:
                    # EC2 Task
                    ec2_mem_charges, ec2_cpu_charges = cost_of_ec2task(task['region'], task['cpu'], task['memory'], task['osType'], task['instanceType'], runTime)
                    ec2_service_mem_cost += ec2_mem_charges
                    ec2_service_cpu_cost += ec2_cpu_charges
            except Exception as e:
                print(e)

    return(fargate_service_cpu_cost, fargate_service_mem_cost, ec2_service_mem_cost, ec2_service_cpu_cost)
