#!/bin/sh
# cd /Users/tazija/Projects/nosql-research/working/
# xattr -d com.apple.quarantine ./amazon-ec2.sh

clear

scripts=("./prepare-node.sh")
nodes=("ycsb-client" "ycsb-client1" "ycsb-client2" "ycsb-node-router" "ycsb-node1" "ycsb-node2" "ycsb-node3" "ycsb-node4")

for (( i=0; i < ${#nodes[@]}; i++ ));
do
    node=${nodes[$i]}
    echo "Initializing $node"
    for (( j=0; j < ${#scripts[@]}; j++ ));
    do
        script=${scripts[$j]}
        source=`less $script`
        echo "[Started] Executing $script"
        ssh -i ssh/pk.pem ec2-user@$node "$source"
        echo "[Completed] Executing $script"
    done
    echo "[Started] Copying X.509"
    scp -r -i ssh/pk.pem ssh/X.509 ec2-user@$node:X.509
    echo "[Completed] Copying X.509"
done


