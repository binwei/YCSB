#!/bin/sh
sudo yum install -y mdadm xfsprogs

export EC2_PRIVATE_KEY=`ls ~/X.509/pk.pem`
export EC2_CERT=`ls ~/X.509/cert.pem`

export EC2_INSTANCE_ID=`wget -q -O - http://169.254.169.254/latest/meta-data/instance-id`
export EC2_AVAILABILITY_ZONE=`wget -q -O - http://169.254.169.254/latest/meta-data/placement/availability-zone`

# create volumes in the availabitity zone $EC2_AVAILABILITY_ZONE
for x in {1..4}; do \
    ec2-create-volume --size 15 --availability-zone $EC2_AVAILABILITY_ZONE; \
done > ~/ec2-volumes

# attache the volumes to the instance $EC2_INSTANCE_ID
i=0; \
for volume in $(awk '{print $2}' ~/ec2-volumes); do \
    i=$(( i + 1 )); \
    if [ -z "$DEVICES" ]; then export DEVICES="/dev/sdh${i}"; else export DEVICES="$DEVICES /dev/sdh${i}"; fi; \
    ec2-attach-volume $volume -i $EC2_INSTANCE_ID -d /dev/sdh${i}; \
done

sleep 10
sudo mdadm --create /dev/md0 --level=raid0 --metadata=1.2 --chunk=256 --raid-devices=4 /dev/sdh*
cat /proc/mdstat
sudo mdadm --detail /dev/md0

sudo blockdev --setra 65536 /dev/md0
sudo mkdir /etc/mdadm/
sudo mdadm -Es | sudo tee /etc/mdadm/mdadm.conf
sudo mkfs.xfs /dev/md0

# Auto-mounting on reboot
echo "/dev/md0 /raid xfs noatime,noexec,nodiratime 0 0" | sudo tee -a /etc/fstab

# Mount and format the drive
sudo mkdir /raid
sudo chown -R ec2-user /raid
sudo mount /raid

df -h /raid
# echo $((30*1024)) > /proc/sys/dev/raid/speed_limit_min
