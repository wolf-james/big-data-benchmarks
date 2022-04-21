# Fake Streamer

## Spark K8s Setup

```shell
helm repo add bitnami https://charts.bitnami.com/bitnami
helm install spark bitnami/spark
helm install spark --set service.type=LoadBalancer --set service.loadBalancerIP=localhost --set metrics.enabled=true bitnami/spark
```

## Streamer Example

Populate `Server.scala` with the required AWS values. Then run the server.

With the Spark cluster running, run the `SparkClient.scala` program after the server has been started.
