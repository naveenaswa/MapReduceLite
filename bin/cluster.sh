#!/bin/bash

typeOfEC2="m3.xlarge"
keyName="MAPR"
yourBucketName="hua9"
keyPath="/home/radioer/CS6240/MAPR.pem"
javaPath="../src/mapreduce/*.java"
userPath="../example/A5/src/*.java"
numberOfNodes=$2

if [ $1 == "c" ] || [ $1 == "a" ] ; then
	#Check whether the two security groups is exist, if not create one
	aws ec2 describe-security-groups --group-names {"mapreduceFrameworkSlave","mapreduceFrameworkMaster"} 2>/dev/null
	if [ $? == "255" ]; then
		secGroupIDM=$(aws ec2 create-security-group --group-name mapreduceFrameworkMaster --description "mapreduceFramework master security group" | ./sid.py)
		aws ec2 authorize-security-group-ingress --group-name mapreduceFrameworkMaster --protocol tcp --port 22 --cidr 0.0.0.0/0

		secGroupIDS=$(aws ec2 create-security-group --group-name mapreduceFrameworkSlave --description "mapreduceFramework slave security group" | ./sid.py)
		aws ec2 authorize-security-group-ingress --group-name mapreduceFrameworkSlave --protocol tcp --port 0-65535 --source-group $secGroupIDM
		aws ec2 authorize-security-group-ingress --group-name mapreduceFrameworkSlave --protocol tcp --port 22 --cidr 0.0.0.0/0

		aws ec2 authorize-security-group-ingress --group-name mapreduceFrameworkMaster --protocol tcp --port 0-65535 --source-group $secGroupIDM
	fi
	#Create cluster
		aws emr create-cluster --release-label emr-4.4.0  --service-role EMR_DefaultRole \
			--ec2-attributes "{\"InstanceProfile\":\"EMR_EC2_DefaultRole\", \"KeyName\":\"$keyName\", \
		\"EmrManagedMasterSecurityGroup\":\"$secGroupIDM\", \
		\"EmrManagedSlaveSecurityGroup\":\"$secGroupIDS\"}" \
		--instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType=$typeOfEC2 \
		InstanceGroupType=CORE,InstanceCount=$numberOfNodes,InstanceType=$typeOfEC2 \
		--region us-east-1  --no-auto-terminate --name A09 \
		--log-uri 's3://$(yourBucketName)/elasticmapreduce/' | ./cid.py > cid
        clusterID=`cat cid`

        while true; do
                state=$(aws emr describe-cluster --cluster-id `cat cid` | ./cluster.py)
                if [ $state == "WAITING" ]; then
                        break
                fi
                echo "WAITING"
                sleep 5
        done

fi

if [ $1 == "a" ] || [ $1 == "s" ] ; then
	clusterID=`cat cid`

	#Get private and public IP addresses for the cluster
	masterIP=`aws emr list-instances --instance-group-types "MASTER" --cluster-id $clusterID | ./nodeips.py`
	masterIPP=`aws emr list-instances --instance-group-types "MASTER" --cluster-id $clusterID | ./nodeipps.py`

	declare -a nodeIP
	ips=$(aws emr list-instances --instance-group-types "CORE" --cluster-id $clusterID | ./nodeips.py)
	ii=1
	nodeIP1=""
	for i in $ips; do
		nodeIP[$ii]=$i
		((ii++))
		nodeIP1="${nodeIP1}$i 9011 "
	done
	declare -a nodeIPP
        ips=$(aws emr list-instances --instance-group-types "CORE" --cluster-id $clusterID | ./nodeipps.py)
        ii=1
        for i in $ips; do
                nodeIPP[$ii]=$i
                ((ii++))
        done

	#Clean up
	aws s3 rm --recursive  s3://${yourBucketName}/output
	aws s3 rm --recursive  s3://${yourBucketName}/shuffle
	aws s3 rm --recursive  s3://${yourBucketName}/mapreduce

	#Compress class files
	ls ${javaPath} | xargs javac -d ../ -cp ../libs/*:../libs/third-party/lib/*
	rm -f ../mapreduce.jar
	jar -cf ../mapreduce.jar -C ../ mapreduce

	#Generate user jar
	rm -Rf ../user
	mkdir -p ../user
	rm -f ../user.jar
	ls ${userPath} | xargs javac -d ../user -cp ../libs/*:../libs/third-party/lib/*:../mapreduce.jar
	jar -cf ../user.jar -C ../user .

	#Generate IP_Detail.txt and add it to jar
	rm -f ../IP_Details.txt
	touch ../IP_Details.txt
	echo "$masterIP,9001">> ../IP_Details.txt
	for i in ${nodeIP[@]}; do
		echo "$i,9011">> ../IP_Details.txt 
	done

	#Upload jar to s3
	aws s3 cp ../mapreduce.jar s3://${yourBucketName}/mapreduce/mapreduce.jar
        aws s3 cp ../IP_Details.txt s3://${yourBucketName}/mapreduce/IP_Details.txt
	aws s3 cp ../user.jar s3://${yourBucketName}/mapreduce/user.jar
	aws s3 cp ../libs s3://${yourBucketName}/mapreduce/libs --recursive

	#Prepare the computing node
	for i in ${nodeIPP[@]}; do
		ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $keyPath ec2-user@$i "rm -Rf mapreduce; mkdir mapreduce; \
		aws s3 cp s3://${yourBucketName}/mapreduce ./mapreduce --recursive; \
		cd mapreduce; PATH=$PATH:/sbin; nohup java -cp ./*:./libs/*:./libs/third-party/lib/* TestMain >out 2>err </dev/null &"
	done 
	
	#Start the controller node
	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i ~/CS6240/MAPR.pem ec2-user@$masterIPP "rm -Rf mapreduce; aws s3 cp s3://${yourBucketName}/mapreduce ./mapreduce --recursive; cd mapreduce; PATH=$PATH:/sbin;java -cp ./*:./libs/*:./libs/third-party/lib/* TestMain"

fi

if [ $1 == "e" ]; then
	aws emr terminate-clusters --cluster-ids `cat cid`	
fi

if [ $1 == "k" ]; then
        clusterID=`cat cid`
        masterIPP=`aws emr list-instances --instance-group-types "MASTER" --cluster-id $clusterID | ./nodeipps.py`
        ips=$(aws emr list-instances --instance-group-types "CORE" --cluster-id $clusterID | ./nodeipps.py)
        for i in $ips; do
            	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $keyPath ec2-user@$i 'netstat -tlpn | grep 9011 | awk '"'"'{print $7}'"'"'| cut -d/ -f1 | xargs kill; rm -Rf mapreduce'
        done
	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $keyPath ec2-user@$masterIPP 'netstat -tlpn | grep 9001 | awk '"'"'{print $7}'"'"'| cut -d/ -f1 | xargs kill; rm -Rf mapreduce'
fi
if [ $1 == "ip" ]; then
	clusterID=`cat cid`
	masterIPP=`aws emr list-instances --instance-group-types "MASTER" --cluster-id $clusterID | ./nodeipps.py`
	ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $keyPath ec2-user@$masterIPP "cd mapreduce; echo master"
	ips=$(aws emr list-instances --instance-group-types "CORE" --cluster-id $clusterID | ./nodeipps.py)
	for i in $ips; do
		echo $i
		ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $keyPath ec2-user@$i "cd mapreduce; echo out; tail -n 10 out; echo err; tail -n 10 err"
	done
fi

