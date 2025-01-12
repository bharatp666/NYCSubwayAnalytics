#!/bin/bash

# Directory to store JARs locally on each node
LOCAL_JARS_DIR="/usr/lib/spark/jars"

# Create the directory if it doesn't exist
mkdir -p ${LOCAL_JARS_DIR}

# Array of JAR URLs
JAR_URLS=(
  "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar"
  "https://repo1.maven.org/maven2/com/google/cloud/spark/spark-bigquery-with-dependencies_2.12/1.2.0/spark-bigquery-with-dependencies_2.12-1.2.0.jar"
  "https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar
"

)

# Download each JAR to the local JARS directory
for JAR_URL in "${JAR_URLS[@]}"; do
  wget -q ${JAR_URL} -P ${LOCAL_JARS_DIR}/
done

# Construct spark.jars property value by concatenating all JAR paths
SPARK_JARS=$(printf ",%s" ${LOCAL_JARS_DIR}/*.jar)
SPARK_JARS=${SPARK_JARS:1} # Remove leading comma

# Update spark-defaults.conf with Spark properties
cat <<EOF >> /etc/spark/conf/spark-defaults.conf
spark.jars=${SPARK_JARS}
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
EOF

# Install Python packages
PIP_EXEC=$(which pip)
${PIP_EXEC} install great-expectations==1.3.0
${PIP_EXEC} install delta-spark==2.3.0
${PIP_EXEC} install google-cloud-logging==3.7.0

