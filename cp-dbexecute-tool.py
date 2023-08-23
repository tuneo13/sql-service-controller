import boto3
import csv
import sys
import os
import pandas as pd

def mainFunction():

    csvFile = os.environ['Server List - CSV']
    mainList = sortServerList(csvFile)

    for index, mainServerList in enumerate(mainList):
        if index == 0:
            sendCommandSettings(mainServerList,'us-east-1')
        elif index == 1:
            sendCommandSettings(mainServerList,'us-west-2')
        elif index == 2:
            sendCommandSettings(mainServerList,'ca-central-1')
        elif index == 3:
            sendCommandSettings(mainServerList,'eu-central-1')
        elif index == 4:
            sendCommandSettings(mainServerList,'eu-west-1')
        elif index == 5:
            sendCommandSettings(mainServerList,'ap-southeast-1')
        elif index == 6:
            sendCommandSettings(mainServerList,'ap-southeast-2')

def sendCommandSettings(sendServerList,sendRegion):
    if not sendServerList:
        print ("No server provided in this region: " + sendRegion)
    else:
        setSSMCommandSetting(sendServerList,sendRegion)

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
    if region == 'CACE':
        return "ca-central-1"
    elif region == 'EUWE':
        return "eu-west-1"
    elif region == 'EUCE':
        return "eu-central-1"     
    if region == 'APSP':
        return "ap-southeast-1"
    elif region == 'APAU':
        return "ap-southeast-2" 

def sortServerList(sortCSVfile):

    filter_df = pd.read_csv(sortCSVfile)
    filterProduct = filter_df.query('servertype.str.contains("DB")')

   
    sortUSEAInstances = []
    sortUSWEInstances = []
    sortCACEInstances = []
    sortEUWEInstances = []
    sortEUCEInstances = []
    sortAPSPInstances = []
    sortAPAUInstances = []
    
    for index, row in filterProduct.iterrows():
        rowServerName = row['servername']
        rowInstanceId = row['instanceid']
        rowRegion = rowServerName[0:4]

        sortRegion = caseRegion(rowRegion)

        sortServerExist = verInstance(rowInstanceId,sortRegion)
        sortSSMStatus = verInstanceSSMStatus(rowInstanceId,sortRegion)
        if sortServerExist == "running" and sortSSMStatus == "Online":

            if rowRegion == "USEA":
                sortUSEAInstances.append(rowInstanceId)
            elif rowRegion == "USWE":
                sortUSWEInstances.append(rowInstanceId)
            elif rowRegion == "CACE":
                sortCACEInstances.append(rowInstanceId)
            elif rowRegion == "EUCE":
                sortEUCEInstances.append(rowInstanceId)
            elif rowRegion == "EUWE":
                sortEUWEInstances.append(rowInstanceId)
            elif rowRegion == "APSP":
                sortAPSPInstances.append(rowInstanceId)
            elif rowRegion == "APAU":
                sortAPAUInstances.append(rowInstanceId)
    sortList = [sortUSEAInstances, sortUSWEInstances, sortCACEInstances, sortEUCEInstances, sortEUWEInstances, sortAPSPInstances, sortAPAUInstances]
    return sortList



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
