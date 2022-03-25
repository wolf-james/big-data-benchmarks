# Fake Streamer

## Spark K8s Setup

```shell
helm3 repo add bitnami https://charts.bitnami.com/bitnami
helm3 install spark bitnami/spark
helm3 install spark --set service.type=LoadBalancer --set service.loadBalancerIP=localhost bitnami/spark
```

### Port-Forwarding

```shell
kubectl port-forward spark-master-0 8080:8080
```

## Streamer Example

Populate `Server.scala` with the required AWS values. Then run the server.

With the Spark cluster running, run the `SparkClient.scala` program after the server has been started.
