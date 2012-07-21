#!/bin/sh

wget http://download.oracle.com/otn-pub/java/jdk/6u33-b03/jdk-6u33-linux-x64.bin
chmod a=rwx ./jdk-6u33-linux-x64.bin
./jdk-6u33-linux-x64.bin
sudo mv ./jdk1.6.0_33 /usr/lib/jvm/jdk1.6.0_33
rm jdk-6u33-linux-x64.bin

sudo alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 2
sudo alternatives --config java

wget http://apache.mirrors.pair.com/maven/binaries/apache-maven-3.0.4-bin.tar.gz
chmod 700 apache-maven-3.0.4-bin.tar.gz
tar xzf apache-maven-3.0.4-bin.tar.gz
rm apache-maven-3.0.4-bin.tar.gz
sudo mv ./apache-maven-3.0.4 /usr/lib
cd /usr/lib
sudo ln -s ./apache-maven-3.0.4 apache-maven

sudo yum -y install git

sudo nano ~/.bash_profile
# JAVA_HOME=/usr/lib/jvm/jdk1.6.0_33
# M2_HOME=/usr/lib/apache-maven

# export JAVA_JOME
# export M2_JOME

git clone git@github.com:Altoros/NoSQL-research.git

