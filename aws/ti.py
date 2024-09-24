import boto3
#import aws_google_auth
#import json
#import time
import os

instance_list = []
allocation_list = []
session = boto3.Session(profile_name='default')
client = session.client('ec2', region_name='us-east-1')
ec2 = boto3.resource('ec2', region_name='us-east-1')
path = "."
directories = [x for x in os.listdir(path) if os.path.isdir(os.path.join(path,x)) and x[0]!='.']
print("Select the set of VMs that you wish to terminate.")

i = 0
dir_list = []
for x in directories:
    dir = directories[i]
    dir_set = dir.split('/')
    if len(dir_set) > 0:
        print(str(i + 1)+".) " + dir_set[i])
        dir_list.append(dir_set[i])
    i+=1
term_resp = input()

instance_name = dir_list[int(term_resp) -1]
print("Terminate all instances associated with " + instance_name + "? (y/n)")
resp = input()
if resp == 'y':
	print("Terminating all instances for {0}".format(instance_name))
	fi = open(instance_name + "/instances.txt","r")
	instance_list=[x.strip() for x in fi.readlines()]
	if os.path.exists(instance_name + "/adnin_instances.txt"):
		fia = open(instance_name + "/admin_instances.txt")
		admin_instance_list=[x.strip() for y in fia.readlines()]
		all_instance_list = instance_list + admin_instance_list
	else:
		all_instance_list = instance_list
	response = client.terminate_instances(InstanceIds=all_instance_list)
	allTerminated = False
	print("Waiting for instances to terminate")
	while allTerminated == False:
		response = client.describe_instances(InstanceIds=all_instance_list)
		running_list = []
		for x in range(len(instance_list)):
			if response['Reservations'][x]['Instances'][0]['State']['Name'] == "terminated":
				running_list.append(True)
			else:
				running_list.append(False)
		if False in running_list:
			allTerminated = False
		else:
			allTerminated = True

	if os.path.exists(instance_name + "/allocation.txt"):
		print("Releasing ElasticIPs")
		fa = open(instance_name + "/allocation.txt","r")
		allocation_list = [x.strip() for x in fa.readlines()]
		for y in allocation_list:
			response = client.release_address(AllocationId=y)
	print("Deleting logs")
	if os.path.exists(instance_name + "/adminIps.txt"):
		os.remove(instance_name + "/adminIps.txt")
	if os.path.exists(instance_name + "/allocation.txt"):
		os.remove(instance_name + "/allocation.txt")
	if os.path.exists(instance_name + "/admin_instances.txt"):
		os.remove(instance_name + "/admin_instances.txt")	
	if os.path.exists(instance_name + "/association.txt"):
		os.remove(instance_name + "/association.txt")
	if os.path.exists(instance_name + "/instances.txt"):
		os.remove(instance_name + "/instances.txt")
	if os.path.exists(instance_name + "/privateIps.txt"):
		os.remove(instance_name + "/privateIps.txt")
	if os.path.exists(instance_name + "/publicips.txt"):
		os.remove(instance_name + "/publicips.txt")
	if os.path.exists(instance_name + "/" + instance_name + ".csv"):
		os.remove(instance_name + "/" + instance_name + ".csv")
	if os.path.exists(instance_name + "/testResult.txt"):
		os.remove(instance_name + "/testResult.txt")
	os.rmdir(instance_name)
	print("Done")
 
