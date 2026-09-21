# Kafka przekazuje zmiany z Debezium do procesu ETL.
resource "kubernetes_deployment" "kafka" {
  metadata {
    name      = "kafka-broker"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "kafka-broker"
      }
    }
    template {
      metadata {
        labels = {
          app = "kafka-broker"
        }
      }
      spec {
        container {
          name  = "zookeeper"
          image = "confluentinc/cp-zookeeper:7.3.0"
          env {
            name  = "ZOOKEEPER_CLIENT_PORT"
            value = "2181"
          }
          env {
            name  = "ZOOKEEPER_TICK_TIME"
            value = "2000"
          }
          port {
            container_port = 2181
          }
        }
        
        container {
          name  = "kafka"
          image = "confluentinc/cp-kafka:7.3.0"
          env {
            name  = "KAFKA_BROKER_ID"
            value = "1"
          }
          env {
            name  = "KAFKA_ZOOKEEPER_CONNECT"
            value = "localhost:2181"
          }
          env {
            name  = "KAFKA_ADVERTISED_LISTENERS"
            value = "PLAINTEXT://kafka-broker:9092"
          }
          env {
            name  = "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR"
            value = "1"
          }
          port {
            container_port = 9092
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "kafka_svc" {
  metadata {
    name      = "kafka-broker"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  spec {
    selector = {
      app = "kafka-broker"
    }
    port {
      port        = 9092
      target_port = 9092
    }
  }
}