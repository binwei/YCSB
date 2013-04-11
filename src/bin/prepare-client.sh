# sudo nano /etc/sudoers

sudo yum -y update
sudo yum -y install git
wget http://www.us.apache.org/dist/maven/binaries/apache-maven-3.0.4-bin.tar.gz
chmod 700 apache-maven-3.0.4-bin.tar.gz
tar xzf apache-maven-3.0.4-bin.tar.gz
#rm -f apache-maven-3.0.4-bin.tar.gz
sudo mv ./apache-maven-3.0.4/ /usr/lib
sudo ln -s /usr/lib/apache-maven-3.0.4/ /usr/lib/apache-maven

wget http://dl.dropbox.com/u/14846803/jdk-6u33-linux-x64.bin
chmod a=rwx ./jdk-6u33-linux-x64.bin
./jdk-6u33-linux-x64.bin
rm ./jdk-6u33-linux-x64.bin
sudo mv ./jdk1.6.0_33/ /usr/lib/jvm/jdk1.6.0_33

nano ~/.bash_profile

# Add following to the end of file
M2_HOME=/usr/lib/apache-maven
JAVA_HOME=/usr/lib/jvm/jdk1.6.0_33
PATH=$PATH:$M2_HOME/bin:$JAVA_HOME/bin

export M2_HOME
export JAVA_HOME
export PATH

# Re-login and configure alternatives
sudo alternatives --install /usr/bin/java java $JAVA_HOME/bin/java 2
sudo alternatives --config java

# Check existing keys
ls -al ~/.ssh
# Generate private-public key pair if required
ssh-keygen
# Add public key to GitHub
less ~/.ssh/id_rsa.pub

git clone git@github.com:Altoros/NoSQL-research.git

