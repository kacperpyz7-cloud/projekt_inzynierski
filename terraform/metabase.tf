# Metabase służy do przeglądania danych analitycznych.
resource "kubernetes_deployment" "metabase" {
  metadata {
    name      = "metabase"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        app = "metabase"
      }
    }

    template {
      metadata {
        labels = {
          app = "metabase"
        }
      }

      spec {
        container {
          name  = "metabase"
          image = "metabase/metabase:latest"
          
          port {
            container_port = 3000
          }

          env {
            name  = "MB_DB_FILE"
            value = "/metabase-data/metabase.db"
          }

          volume_mount {
            name       = "metabase-storage"
            mount_path = "/metabase-data"
          }

          resources {
            requests = {
              memory = "512Mi"
              cpu    = "250m"
            }
            limits = {
              memory = "2Gi"
              cpu    = "1000m"
            }
          }
        }

        volume {
          name = "metabase-storage"
          empty_dir {}
        }
        }
      }
    }
  }


resource "kubernetes_service" "metabase_service" {
  metadata {
    name      = "metabase-service"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }

  spec {
    selector = {
      app = "metabase"
    }

    port {
      port        = 3000
      target_port = 3000
      node_port   = 30030
    }

    type = "NodePort"
  }
}