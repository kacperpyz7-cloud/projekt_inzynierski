# Dane dostępowe są przekazywane do podów przez Secret Kubernetes
resource "kubernetes_secret" "db_credentials" {
  metadata {
    name      = "db-credentials"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  
  data = {
    username = "admin"
    password = var.db_password
  }
  
  type = "Opaque"
}

# Generator tworzy testowe odczyty dla aktywnych liczników
resource "kubernetes_deployment" "iot_generator" {
  metadata {
    name      = "iot-generator"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "iot-generator"
      }
    }
    template {
      metadata {
        labels = {
          app = "iot-generator"
        }
      }
      spec {
        container {
          name  = "generator"
          image = "iot-generator:v3"
          
          image_pull_policy = "Never"

          env {
            name  = "DB_HOST"
            value = "postgres-source"
          }
          env {
            name  = "DB_PORT"
            value = "5432"
          }
          env {
            name  = "DB_NAME"
            value = "source_db"
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