#!/bin/sh

echo "Enter AMI node to login to"
read node
ssh -i ssh/pk.pem ec2-user@$node