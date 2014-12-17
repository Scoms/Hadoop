clear
rm -Rf output* 
javac -classpath ${HADOOP_HOME}/hadoop-core-${HADOOP_VERSION}.jar:$HADOOP_HOME/lib/commons-cli-1.2.jar -d projet_classes Projet.java

jar -cvf projet.jar -C projet_classes/ .
