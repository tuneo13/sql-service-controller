import boto3
import csv
import sys
import os
import pandas as pd

def mainFunction():

    csvFile = 'cp-automation-tool-server-list.csv'

    main_USEA_list, main_USWE_list = sortServerList(csvFile)

    if not main_USEA_list:
        print ("No USEA server in the list.")
    else:
        setSSMCommandSetting(main_USEA_list,'us-east-1')
    
    if not main_USWE_list:
        print ("No USWE server in the list.")
    else:
        setSSMCommandSetting(main_USWE_list,'us-west-2')

def caseDBExecutionDocument(executionType):
    if executionType == 'START_DB':
        return "sre-db-start"
    elif executionType == 'STOP_DB':
        return "sre-db-shutdown"
    elif executionType == 'HEALTHCHECK_DB':
        return "sre-db-healthcheck"     

def setSSMCommandSetting(set_serverlist,setRegion):

    from datetime import date
    inputServerType = os.environ['Execution Type']
    setDocument = caseDBExecutionDocument(inputServerType)
    ssm_client = boto3.client('ssm',region_name=setRegion)
    runToday = date.today()
    runToday = runToday.strftime("%d%m%Y")
    setComment = (inputServerType + "-" + runToday)
    for i in range(0, len(set_serverlist), 30):
        limited_serverlist = set_serverlist[i : i + 30]
        print(limited_serverlist)
        runSSMCommand(ssm_client,limited_serverlist,setComment,setDocument)

def verInstanceSSMStatus(verInstanceId,verRegion):
    ssm_client = boto3.client('ssm',region_name=verRegion)
    try:
        response = ssm_client.describe_instance_information(
            Filters=[{
                    'Key': 'InstanceIds',
                    'Values': [
                        verInstanceId,
                    ]
                }
            ]
        )
        return(response['InstanceInformationList'][0]['PingStatus'])
    except:
        print(verInstanceId + " there is a CONNECTION LOST in SSM. Cannot do RUNCommmand.")
        return ("An exception occurred")
        


def verInstance(verInstanceId,verRegion):
    
    try:
        ec2_client = boto3.client('ec2',region_name=verRegion)
        response = ec2_client.describe_instance_status(
            InstanceIds=[verInstanceId],
        )
        verStatus = (response['InstanceStatuses'][0]['InstanceState']['Name'])
        return verStatus
    except:
        print(verInstanceId + " has an issue with this instance.")
        return ("An exception occurred")


def caseRegion(region):
    if region == 'USEA':
        return "us-east-1"
    elif region == 'USWE':
        return "us-west-2"
    elif region == 'ALL':
        return "all"     

def sortServerList(sortCSVfile):

    filter_df = pd.read_csv(sortCSVfile)
    filterProduct = filter_df.query('servertype.str.contains("DB")')


    sortUSEAInstances = []
    sortUSWEInstances = []


    for index, row in filterProduct.iterrows():
        rowServerName = row['servername']
        rowInstanceId = row['instanceid']
        rowRegion = rowServerName[0:4]

        sortRegion = caseRegion(rowRegion)

        sortServerExist = verInstance(rowInstanceId,sortRegion)
        if sortServerExist == "running" and rowRegion == "USEA":
            sortSSMStatus = verInstanceSSMStatus(rowInstanceId,sortRegion)
            if sortSSMStatus == "Online":
                sortUSEAInstances.append(rowInstanceId)
            
        elif sortServerExist == "running" and rowRegion == "USWE":
            sortSSMStatus = verInstanceSSMStatus(rowInstanceId,sortRegion)
            if sortSSMStatus == "Online":
                sortUSWEInstances.append(rowInstanceId)
            
    
    return sortUSEAInstances,sortUSWEInstances



def runSSMCommand(runSession,runInstances,runComment,runDocument):
    from datetime import date
    

    
    response = runSession.send_command(
                    Targets=[{"Key": "InstanceIds", "Values": runInstances}],
                    Comment=runComment,
                    DocumentName=runDocument,
                    MaxConcurrency='100%',
                    MaxErrors='100%',
                    TimeoutSeconds=900)

    command_id = response['Command']['CommandId']
    print("Command Id:" + command_id)

mainFunction()