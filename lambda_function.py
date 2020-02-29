import json
import sys
import boto3
from boto3.session import Session
import datetime
import mysql.connector
from mysql.connector import errorcode



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


def save_data(taskDefinition,db):
    mycursor = db.cursor()
    sql = "INSERT INTO task_definitions VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (taskDefinition['taskArn'], taskDefinition['clusterArn'], taskDefinition['containerInstanceArn'], taskDefinition['cpu'], taskDefinition['group'], taskDefinition['groupName'], taskDefinition['instanceId'], taskDefinition['instanceType'], taskDefinition['launchType'], taskDefinition['memory'], taskDefinition['osType'], taskDefinition['region'], taskDefinition['runTime'], taskDefinition['startedAt'],  taskDefinition['stoppedAt'])
    mycursor.execute(sql, val)
    db.commit()


def update_task_runtime(taskArn, stoppedAt, runTime, db):
    print("updating record")
    mycursor = db.cursor()
    sql = "UPDATE task_definitions set stoppedAt = '"+str(stoppedAt)+"', runTime =  '"+str(runTime)+"' WHERE taskArn = '"+str(taskArn)+"'"
    mycursor.execute(sql)
    db.commit()

def getTask(taskArn, db):
    data = {}
    sql  = "SELECT * FROM task_definitions WHERE taskArn = '"+taskArn+"'"
    mycursor = db.cursor()
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    print(myresult)
    return myresult


def lambda_handler(event, context):
    print('Here is the event:')
    print(json.dumps(event))
    id_name = "taskArn"
    database = getConnection()
    new_record = {}
    # For debugging so you can see raw event format.
   
    if event["source"] != "aws.ecs" and event["detail-type"] != "ECS Task State Change":
        raise ValueError("Function only supports input from events with a source type of: aws.ecs and of type - ECS Task State Change -")

    if event["detail"]["lastStatus"] == event["detail"]["desiredStatus"]:
        event_id = event["detail"]["taskArn"]

        s = Session()
        cur_region = s.region_name
        saved_event = getTask(event_id, database)
        
        # Look first to see if you have received this taskArn before.
        # If not,
        #   - you are getting a new task that has just started, or the Lambda solution was deployed
        #     after the task started and it is being stopped now.
        #   - store its details in DDB
        # If yes,
        #   - that just means that you are receiving a task change - mostly a stop event.
        #   - store the stop time in the task item in DDB
        if len(saved_event) > 0:
            if event["detail"]["lastStatus"] == "STOPPED":
                update_task_runtime(event_id, str(event["detail"]["stoppedAt"]), getRunTime(event["detail"]["startedAt"], event["detail"]["stoppedAt"]), database)
                print("Saving updated event - ID " + event_id)
        else:
            # This could be if the task has just started, or
            # The Lambda is deployed after the task has started running.
            #   In this case, the task event will only be raised when it is stopped.
            new_record["launchType"]    = event["detail"]["launchType"]
            new_record["region"]        = event["region"]
            new_record["clusterArn"]    = event["detail"]["clusterArn"]
            new_record["cpu"]           = event["detail"]["cpu"]
            new_record["memory"]        = event["detail"]["memory"]
            if new_record["launchType"] == 'FARGATE':
                new_record["containerInstanceArn"]  = 'INSTANCE_ID_UNKNOWN'
                (new_record['instanceType'], new_record['osType'], new_record['instanceId']) = ('INSTANCE_TYPE_UNKNOWN', 'linux', 'INSTANCE_ID_UNKNOWN')
            else:
                new_record["containerInstanceArn"]  = event["detail"]["containerInstanceArn"]
                (new_record['instanceType'], new_record['osType'], new_record['instanceId']) = getInstanceType(event['region'], event['detail']['clusterArn'], event['detail']['containerInstanceArn'], event['detail']['launchType'])

            if ':' in event["detail"]["group"]:
                new_record["group"], new_record["groupName"] = event["detail"]["group"].split(':')
            else:
                new_record["group"], new_record["groupName"] = 'taskgroup', event["detail"]["group"]

            # Not provided in FARGATE - new_record["pullStartedAt"] = event["detail"]["pullStartedAt"]
            new_record["startedAt"]     = event["detail"]["startedAt"]
            new_record["taskArn"]       = event_id
            new_record['stoppedAt'] = 'STILL-RUNNING'
            new_record['runTime'] = 0

            if event["detail"]["lastStatus"] == "STOPPED":
                new_record['stoppedAt']     = event["detail"]["stoppedAt"]
                new_record['runTime']       = getRunTime(event["detail"]["startedAt"], event["detail"]["stoppedAt"])
                        
            save_data(new_record, database)
            print("Saving new event - ID " + event_id)


def getInstanceType(region, cluster, instance, launchType):
    instanceType    = 'INSTANCE_TYPE_UNKNOWN'
    osType          = 'linux'
    instanceId      = 'INSTANCE_ID_UNKNOWN'
    
    # Shouldnt care about isntanceType if this is a FARGATE task
    if launchType == 'FARGATE':
        return (instanceType, osType, instanceId)
    
    ecs = boto3.client("ecs")
    try:
        result = ecs.describe_container_instances(cluster=cluster, containerInstances=[instance])
        if result and 'containerInstances' in result:
            attr_dict = result['containerInstances'][0]['attributes']
            
            instanceId = result['containerInstances'][0]["ec2InstanceId"]
            
            instance_type = [d['value'] for d in attr_dict if d['name'] == 'ecs.instance-type']
            if len(instance_type):
                # Return the instanceType. In addition, store this value in a DynamoDB table.
                instanceType = instance_type[0]
            
            os_type = [d['value'] for d in attr_dict if d['name'] == 'ecs.os-type']
            if len(os_type):
                # Return the osType. In addition, store this value in a DynamoDB table.
                osType = os_type[0]
            
        # Else - if describe_instances doesnt return a result, make a last attempt check in DynamoDB table
        # that keeps a mapping of containerInstanceARN to instanceType
        return (instanceType, osType, instanceId)
    except:
        # Try finding the instanceType in DynamoDB table
        return (instanceType, osType, instanceId)
        
def getRunTime(startTime, stopTime):
    runTime = '0.0'
    start = datetime.datetime.strptime(startTime, '%Y-%m-%dT%H:%M:%S.%fZ')
    stop = datetime.datetime.strptime(stopTime, '%Y-%m-%dT%H:%M:%S.%fZ')
    runTime = (stop-start).total_seconds()
    return int(round((runTime)))
