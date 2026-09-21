# Debezium publikuje zmiany z bazy źródłowej w Kafka.
resource "kubernetes_deployment" "debezium" {
  metadata {
    name      = "debezium-connect"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  spec {
    replicas = 1
    selector {
      match_labels = {
        app = "debezium-connect"
      }
    }
    template {
      metadata {
        labels = {
          app = "debezium-connect"
        }
      }
      spec {
        container {
          name  = "debezium"
          image = "debezium/connect:2.5"
          
          env {
            name  = "BOOTSTRAP_SERVERS"
            value = "kafka-broker:9092"
          }
          env {
            name  = "GROUP_ID"
            value = "1"
          }
          env {
            name  = "CONFIG_STORAGE_TOPIC"
            value = "my_connect_configs"
          }
          env {
            name  = "OFFSET_STORAGE_TOPIC"
            value = "my_connect_offsets"
          }
          env {
            name  = "STATUS_STORAGE_TOPIC"
            value = "my_connect_statuses"
          }
          port {
            container_port = 8083
          }
        }
      }
    }
  }
}

# NodePort pozwala wysłać konfigurację konektora do klastra.
resource "kubernetes_service" "debezium_svc" {
  metadata {
    name      = "debezium-connect"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  spec {
    selector = {
      app = "debezium-connect"
    }
    
    type = "NodePort"
    
    port {
      port        = 8083
      target_port = 8083
      node_port   = 30083
    }
  }
}

# Job czeka na gotowe API i tworzy konektor po uruchomieniu Debezium.
resource "kubernetes_job" "debezium_configurator" {
  metadata {
    name      = "debezium-configurator"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  wait_for_completion = false
  depends_on = [
    kubernetes_deployment.debezium,
    kubernetes_service.debezium_svc
    
  ]
  
  spec {
    backoff_limit = 5
    
    template {
      metadata {
        labels = {
          app = "debezium-configurator"
        }
      }
      spec {
        restart_policy = "OnFailure"
        container {
          name  = "curl-injector"
          image = "curlimages/curl:latest"
          
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
#Konfiguracja konektora Debezium do PostgreSQL
          command = ["/bin/sh", "-c"]
          args = [
            <<-EOT
            set -e
            echo "Czekam na uruchomienie API Debezium..."
            
            RETRY_COUNT=0
            MAX_RETRIES=12
            while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
              echo "Próba $((RETRY_COUNT+1))/$MAX_RETRIES - Sprawdzam dostęp do Debezium..."
              if curl -s -f -o /dev/null http://debezium-connect:8083 2>/dev/null; then
                echo "API Debezium gotowe."
                break
              fi
              RETRY_COUNT=$((RETRY_COUNT+1))
              if [ $RETRY_COUNT -lt $MAX_RETRIES ]; then
                sleep 5
              fi
            done
            
            if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
              echo "Błąd: API Debezium nie odpowiada po 60 sekundach."
              exit 1
            fi
            
            echo "Wysyłam konfigurację konektora CDC..."
            
            OUTPUT_FILE="/tmp/debezium-response.txt"
            HTTP_CODE=$(curl -s -w "%%{http_code}" -X POST \
              -H "Accept:application/json" \
              -H "Content-Type:application/json" \
              -o "$OUTPUT_FILE" \
              http://debezium-connect:8083/connectors \
              -d '{
                "name": "source-postgres-connector",
                "config": {
                  "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                  "tasks.max": "1",
                  "database.hostname": "postgres-source",
                  "database.port": "5432",
                  "database.user": "'"$DB_USER"'",
                  "database.password": "'"$DB_PASSWORD"'",
                  "database.dbname": "source_db",
                  "topic.prefix": "oltp",
                  "plugin.name": "pgoutput",
                  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                  "key.converter.schemas.enable": "false",
                  "value.converter.schemas.enable": "false",
                  "snapshot.mode": "initial",
                  "decimal.handling.mode": "double"
                }
              }')
            
            RESPONSE=$(cat "$OUTPUT_FILE" 2>/dev/null || echo "")
            
            echo "HTTP Status: $HTTP_CODE"
            echo "Response: $RESPONSE"
            
            if [ "$HTTP_CODE" != "201" ] && [ "$HTTP_CODE" != "200" ] && [ "$HTTP_CODE" != "409" ]; then
              echo "Błąd: API zwróciło status $HTTP_CODE"
              exit 1
            fi
            
            echo "Konfiguracja konektora CDC wstrzyknięta pomyślnie."
            EOT
          ]
        }
      }
    }
  }
}