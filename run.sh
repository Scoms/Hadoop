if [ "$#" -lt 1  ] || [ "$#" -gt 2 ]; then
  echo "Usage: $0 <inputFile> [passes]" >&2
  exit 1
fi

mkdir processed
rm -Rf output* 
${HADOOP_HOME}/bin/hadoop jar projet.jar Projet $1 output $2
