#!/bin/bash
export K8S_SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"
$SPARK_HOME/bin/spark-submit \
--master k8s://$K8S_SERVER \
--deploy-mode cluster \
--name pyspark-minikube-test \
--conf spark.kubernetes.namespace= sparktest \
--conf spark.kubernetes.container.image=sparkkubetest:local \
--conf spark.kubernetes.container.image.pullPolicy= IfNotPresent \
--conf spark.executor.instances=2 \
--conf spark.excutor.cores=1 \
--conf spark.driver.cores=1 \
--conf spark.driver.memory=2g \
--conf spark.executor.memory=2g \
--conf spark.kubernetes.driver.requests.cpu=1 \
--conf spark.kubernetes.driver.requests.memory=2g \
--conf spark.kubernetes.executor.requests.cpu=1 \
--conf spark.kubernetes.executor.requests.memory=2g \
--conf spark.kubernetes.driver.limits.cpu=1 \
--conf spark.kubernetes.driver.limits.memory=2g \
--conf spark.kubernetes.executor.limits.cpu=1 \
--conf spark.kubernetes.executor.limits.memory=2g \
--conf spark.kubernetes.driver.pod.name=pyspark-minikube-test-driver \
--conf spark.kubernetes.executor.podNamePrefix=pyspark-minikube-test-executor \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=sparktestservice \
local:///app/app.py
#local:///app/sparktest/app.py