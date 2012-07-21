#!/bin/sh
rm mongodb-linux-x86_64-2.0.6.tgz
curl http://downloads.mongodb.org/linux/mongodb-linux-x86_64-2.0.6.tgz > ~/mongodb-linux-x86_64-2.0.6.tgz
tar xzf ~/mongodb-linux-x86_64-2.0.6.tgz
sudo mv ~/mongodb-linux-x86_64-2.0.6/ /usr/lib/mongodb-2.0.6
rm ~/mongodb-linux-x86_64-2.0.6.tgz
sudo mkdir -p /raid/var/log/mongodb/ /raid/var/lib/mongodb/
sudo ln -s /raid/var/lib/mongodb /var/lib/mongodb
sudo ln -s /raid/var/log/mongodb /var/log/mongodb
sudo chown -R ec2-user /raid/var/lib/mongodb/ /raid/var/log/mongodb/ /var/lib/mongodb/ /var/log/mongodb/
chmod 777 mongo-bin/*.sh

killall -9 mongos mongod
sudo mkdir -p /raid/var/log/mongodb/ /raid/var/lib/mongodb/
sudo chown -R ec2-user /raid/var/lib/mongodb/ /raid/var/log/mongodb/ /var/lib/mongodb/ /var/log/mongodb/
mongo-bin/start-mongod.sh

tail -n 100 /var/log/mongodb/mongodb-data.log