#!/bin/sh

sudo umount /raid
sudo mdadm --stop /dev/md0
sudo mdadm --remove /dev/md0

export EC2_PRIVATE_KEY=`ls ~/X.509/pk.pem`
export EC2_CERT=`ls ~/X.509/cert.pem`

export EC2_INSTANCE_ID=`wget -q -O - http://169.254.169.254/latest/meta-data/instance-id`
export EC2_AVAILABILITY_ZONE=`wget -q -O - http://169.254.169.254/latest/meta-data/placement/availability-zone`
export EC2_REGION=`curl http://169.254.169.254/latest/dynamic/instance-identity/document|grep region|awk -F\" '{print $4}'`

i=0; \
for vol in $(awk '{print $2}' ~/ec2-volumes); do \
i=$(( i + 1 )); \
ec2-detach-volume $vol -i $EC2_INSTANCE_ID -d /dev/sdh${i} -f; \
done

sleep 30

for volume in $(awk '{print $2}' ~/ec2-volumes); do \
    ec2-delete-volume $volume; \
done