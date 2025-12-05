#!/bin/bash

sudo apt update
sudo apt install openjdk-21-jdk -y

JAVA_PATH=$(update-alternatives --query java | grep 'Value: ' | grep -o '/.*/bin/java')
JAVA_HOME=$(update-alternatives --query java | grep 'Value: ' | grep -o '/.*/bin/java' | xargs dirname | xargs dirname)

echo "export JAVA_HOME=$JAVA_HOME" | sudo tee -a /etc/profile

sudo apt install maven -y

MAVEN_VERSION=$(mvn -v | head -n 1 | grep -oP '\d+\.\d+\.\d+')
REQUIRED_VERSION="3.9.0"

version_ge() {
    printf '%s\n%s' "$2" "$1" | sort -V -C
}

if ! version_ge "$MAVEN_VERSION" "$REQUIRED_VERSION"; then
  echo "Installing Maven 3.9.0 (current version: $MAVEN_VERSION)"
  
  sudo apt remove maven -y
  wget https://archive.apache.org/dist/maven/maven-3/3.9.0/binaries/apache-maven-3.9.0-bin.tar.gz -P /tmp
  sudo tar -xvzf /tmp/apache-maven-3.9.0-bin.tar.gz -C /opt
  sudo ln -s /opt/apache-maven-3.9.0/bin/mvn /usr/bin/mvn
fi

MAVEN_HOME=$(mvn -v | grep 'Maven home' | awk '{print $3}')

echo "export MAVEN_HOME=$MAVEN_HOME" | sudo tee -a /etc/profile
echo "export PATH=\$PATH:\$MAVEN_HOME/bin" | sudo tee -a /etc/profile

source /etc/profile

echo "==> Installation complete."
java -version
echo "JAVA_HOME: $JAVA_HOME"
mvn -v
echo "MAVEN_HOME: $MAVEN_HOME"