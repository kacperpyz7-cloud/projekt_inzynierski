# ETL pobiera zdarzenia z Kafka i zapisuje je w bazie docelowej.
resource "kubernetes_deployment" "etl_processor" {
  metadata {
    name      = "etl-processor"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "etl-processor"
      }
    }
    template {
      metadata {
        labels = {
          app = "etl-processor"
        }
      }
      spec {
        container {
          name  = "etl"
          image = "etl-processor:v10"
          image_pull_policy = "IfNotPresent"

          env {
            name  = "KAFKA_BROKER"
            value = "kafka-broker:9092"
          }
          env {
            name  = "DB_HOST"
            value = "postgres-sink"
          }
          env {
            name  = "DB_PORT"
            value = "5432"
          }
          env {
            name  = "DB_NAME"
            value = "sink_db"
          }

          env {
            name = "DB_USER"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_credentials.metadata[0].name
                key  = "username"
              }
            }
          }
          env {
            name = "DB_PASSWORD"
            value_from {
              secret_key_ref {
                name = kubernetes_secret.db_credentials.metadata[0].name
                key  = "password"
              }
            }
          }
        }
      }
    }
  }
}