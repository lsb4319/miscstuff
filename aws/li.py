import boto3
#import aws_google_auth
#import json
import time
import os
import csv
import sys
import subprocess
import datetime

KEY=os.path.expanduser('~/.ssh')
COMMAND = "uname -a"
instanceId = []
adminInstanceId = []
instanceList = []
adminInstanceList = []
allocationList = []
associationList = []
publicIpList = []
privateIpList = []
adminPublicIPList = []
adminPrivateIPList = []
launchTemplateId = ""
launchTemplateVersion = ""
correct = 0
modeLong = ""
session = boto3.Session(profile_name='default')
client = session.client('ec2', region_name='us-east-1')
ec2 = boto3.resource('ec2', region_name='us-east-1')
keyfile="singlestore-key.pem"
space = " "
us = "memSQL"

print("Enter the name of the company for whom the training is to be done.")
company = input()

print("Enter the start date of the course. (yyyymmdd)")
startDate = input()

print("Enter your email alias")
email = input()

print("Enter the number of students")
studentNumber = input()

while correct != 1:
	print("Enter the launch template to use")
	print("1. s2-student-ubuntu-22.04")
	print("2. s2-student-ubuntu-18.04")
	templateNumber = input()
	if int(templateNumber) < 1 or int(templateNumber) > 2:
		correct = 0
		print("The selected template is invalid: try again")
	else:
		correct = 1

correct = 0
while correct != 1:
	print("Enter the mode of the training")
	print("1. Virtual Classroom")
	print("2. In-person")
	mode = input()
	if int(mode) < 1 or int(mode) > 2:
		correct = 0
		print("The selected mode is invalid: try again")
	else:
		correct = 1
	if mode == '1':
		mode = "vc"
	elif mode == '2':
		mode = "ilt"

correct = 0
print("Use Elastic IPs? (y or n)")
elasticIps = input()
while correct == 0:
	if elasticIps == 'y' or elasticIps == 'n' or elasticIps == 'Y' or elasticIps == 'N':
		correct = 1
	else:
		correct = 0
		print("Please select 'y' or 'n'.")

correct = 0
print("Use Admin machine? (y or n)")
useAdmin = input()
while correct == 0:
	if useAdmin == 'y' or useAdmin == 'n' or useAdmin == 'Y' or useAdmin == 'N':
		correct = 1
	else:
		correct = 0
		print("Please select 'y' or 'n'.")

if useAdmin == 'Y' or useAdmin == 'y':
	adminLaunchTemplateId = "lt-098fbd008c1590286"
	adminLaunchTemplateVer = "3"
	adminKeyFile = "singlestore-key"

KEY = os.path.expanduser('~/.ssh/singlestore-key.pem')
keyfile = 'singlestore-key.pem'

if templateNumber == "1":
	launchTemplateId = "lt-0a258638866314809"
	launchTemplateVersion = "5"
	keyfile = "singlestore-key"
elif templateNumber == "2":
	launchTemplateId = "lt-0cb44898c50a5b118"
	launchTemplateVersion = "1"
	keyfile = "singlestore-key"
 
instanceName = startDate+"_"+mode+"_"+company
if useAdmin == 'y' or useAdmin == 'Y':
	numInst = int(studentNumber) * 5
else:
	numInst = int(studentNumber) *4

print("Instance name: " + instanceName)
print("SSH Key: " + KEY)
print("Owner: " + email)
print("Number of Students: " + str(studentNumber))
print("Number of Instances: " + str(numInst))
print("Press enter to continue.")
input()
print("working")
if useAdmin == 'y' or useAdmin == 'Y':
	t1 = datetime.datetime.now()
	print("Create Admin instances started at {0}".format(t1))
	for y in range(int(numInst/4)):
		admin_response = client.run_instances(
			LaunchTemplate={'LaunchTemplateId': adminLaunchTemplateId, 'Version': adminLaunchTemplateVer},
			MinCount=1,
			MaxCount=1,
			KeyName=keyfile,
			#Placement={'AvailabilityZone':"us-east-1f"}
		)
		adminInstanceId=(admin_response ['Instances'][0]['InstanceId'])
		adminInstanceList.append(adminInstanceId)
	t2 = datetime.datetime.now()
	run_time = t2 - t1
	print("Create Admin instances started at {0}".format(t2))
	print("{0} instances in {1}".format(numInst/4,run_time))
t1 = datetime.datetime.now()
print("Create cluster instances started at {0}".format(t1))
for x in range(numInst):
	response = client.run_instances(
		LaunchTemplate={'LaunchTemplateId': launchTemplateId, 'Version': launchTemplateVersion},
		MinCount=1,
		MaxCount=1,
		KeyName=keyfile,
		#Placement={'AvailabilityZone':"us-east-1f"}
	)
	instanceId=(response ['Instances'][0]['InstanceId'])
	instanceList.append(instanceId)
t2 = datetime.datetime.now()
run_time = t2-t1
print("Create cluster instances finished at {0}".format(t2))
print("{0} instances in {1}".format(numInst,run_time))

allRunning = False
print("Waiting for all instances to enter running state.")
while allRunning == False:
	response = client.describe_instances(InstanceIds=instanceList+adminInstanceList)
	runningList = []
	for x in range(len(instanceList)+len(adminInstanceList)):
		if response['Reservations'][x]['Instances'][0]['State']['Name'] != "running":
			runningList.append(False)
		else:
			runningList.append(True)
	if False in runningList:
		allRunning = False
	else:
		allRunning = True
print("Adding tags")
for x in adminInstanceList:
	client.create_tags(
		Resources=[x],
		Tags=[
			{
				'Key': 'Project',
				'Value': instanceName
			},
			{
				'Key': 'Name',
				'Value': instanceName
			},
			{
				'Key': 'Owner',
				'Value': email
			}
		]
	)

for y in instanceList:
	client.create_tags(
		Resources=[y],
		Tags=[
			{
				'Key': 'Project',
				'Value': instanceName
			},
			{
				'Key': 'Name',
				'Value': instanceName
			},
			{
				'Key': 'Owner',
				'Value': email
			}
		]
	)

if elasticIps=='y':
	print("Provisioning Elastic IPs")
	combinedInstanceList = instanceList+adminInstanceList
	for x in range(len(combinedInstanceList)):
		response=client.allocate_address(Domain='vpc')
		allocationId=(response['AllocationId'])
		allocationList.append(allocationId)
	for x in range(len(combinedInstanceList)):
		response = client.associate_address(
			AllocationId=allocationList[x],
			InstanceId=combinedInstanceList[x])
		associationId = (response['AssociationId'])
		associationList.append(associationId)
		
response = client.describe_instances(InstanceIds=instanceList)
for x in range(len(instanceList)):
	publicIpList.append(response['Reservations'][x]['Instances'][0]['PublicIpAddress'])
	privateIpList.append(response['Reservations'][x]['Instances'][0]['PrivateIpAddress'])
response = client.describe_instances(InstanceIds=adminInstanceList)

for x in range(len(adminInstanceList)):
	adminPublicIPList.append(response['Reservations'][x]['Instances'][0]['PublicIpAddress'])
	adminPrivateIPList.append(response['Reservations'][x]['Instances'][0]['PrivateIpAddress'])
response = client.describe_instances(InstanceIds=adminInstanceList)

os.mkdir(instanceName)
print("Writing log files")
f = open(instanceName + "/instances.txt", "w")
for line in instanceList:
	f.write(line)
	f.write(os.linesep)
f.close()

f = open(instanceName + "/admin_instances.txt", "w")
for line in adminInstanceList:
	f.write(line)
	f.write(os.linesep)
f.close()

if elasticIps=='y':
	f = open(instanceName + "/association.txt", "w")
	for line in associationList:
		f.write(line)
		f.write(os.linesep)
	f.close()

if elasticIps=='y':
	f = open(instanceName + "/allocation.txt", "w")
	for line in allocationList:
		f.write(line)
		f.write(os.linesep)
	f.close()
 
f = open(instanceName + "/publicIps.txt", "w")
for line in publicIpList:
	f.write(line)
	f.write(os.linesep)
f.close()

f = open(instanceName + "/privateIps.txt", "w")
for line in privateIpList:
	f.write(line)
	f.write(os.linesep)
f.close()

f = open(instanceName + "/adminIps.txt", "w")
for line in adminPrivateIPList:
	f.write(line)
	f.write(os.linesep)

print("Writing spreadsheet")
if len(privateIpList)==len(publicIpList) and len(adminPrivateIPList)==len(adminPublicIPList):
	with open(instanceName + '/' + instanceName + '.csv',mode='w') as ec2_file:
		ec2_writer = csv.writer(ec2_file, delimiter=',',quotechar='"',quoting=csv.QUOTE_MINIMAL)
		ec2_writer.writerow(['Public IP','Private IP','Assigned To','Cluster Role','Comments'])
		i=0
		j=0
		if useAdmin == 'y' or useAdmin == 'Y': 
			setnum = 0
		else:
			setnum = 1
		for x in range (len(privateIpList) + len(adminPrivateIPList)):
			if setnum == 0:
				ec2_writer.writerow([adminPublicIPList[j],adminPrivateIPList[j],"","Admin",""])
				j+=1
				setnum+=1
			elif setnum == 1:
				ec2_writer.writerow([publicIpList[i],privateIpList[i],"","MA",""])
				i+=1
				setnum += 1
			elif setnum == 2:
				ec2_writer.writerow([publicIpList[i],privateIpList[i],"","CA",""])
				i+=1
				setnum += 1
			elif setnum == 3:
				ec2_writer.writerow([publicIpList[i],privateIpList[i],"","L1",""])
				i+=1
				setnum +=1
			elif setnum == 4:
				ec2_writer.writerow([publicIpList[i],privateIpList[i],"","L2",""])
				i+=1
				if useAdmin == 'y' or useAdmin =='Y':
					setnum = 0
				else:
					setnum = 1
			else:
				sys.exit("setnum variable has invalid value")
else:
	sys.exit("Could not create spreadsheet: Private IPs = {} Public IPs = {} Admin Public IPs = {} Admin Private IPs = {}".format(privateIpList,publicIpList, adminPrivateIPList, adminPublicIPList))

time.sleep(60)
print("testing connectivity with instances")
all_public_ips = publicIpList + adminPublicIPList
f = open(instanceName + "/testResult.txt", "w")
errorCount = 0
successCount = 0
for line in all_public_ips:
	ssh = subprocess.Popen(["ssh","-i", KEY, "-o","StrictHostKeyChecking=accept-new", "{}@{}".format("ubuntu",line), COMMAND], shell=False,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	result = ssh.stdout.readlines()
	if result ==[]:
		error = ssh.stderr.readlines()
		f.write (line + KEY + " Error: %s" % error)
		f.write (os.linesep)
		errorCount+=1
	else:
		f.write (line + " Success")
		f.write (os.linesep)
		successCount+=1
f.close()
print("Connection Test Complete: {} Succeeded {} Failed".format(successCount,errorCount))