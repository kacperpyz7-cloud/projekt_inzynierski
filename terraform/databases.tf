# Schemat i dane początkowe bazy źródłowej.
resource "kubernetes_config_map" "postgres_source_init" {
  metadata {
    name      = "postgres-source-init"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }

  data = {
    "init.sql" = <<EOF
    CREATE TABLE IF NOT EXISTS taryfy (
        id SERIAL PRIMARY KEY,
        nazwa_taryfy VARCHAR(50),
        cena_dzien NUMERIC(5,2),
        cena_noc NUMERIC(5,2),
        godzina_noc_start TIME,
        godzina_noc_koniec TIME,
        znizka_weekend BOOLEAN DEFAULT FALSE
    );

    CREATE TABLE IF NOT EXISTS klienci (
        id SERIAL PRIMARY KEY,
        pesel VARCHAR(11) UNIQUE,
        imie VARCHAR(50),
        nazwisko VARCHAR(50),
        miasto VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS liczniki (
        id SERIAL PRIMARY KEY,
        klient_id INT REFERENCES klienci(id),
        taryfa_id INT REFERENCES taryfy(id),
        mac_address VARCHAR(17) UNIQUE,
        model VARCHAR(50),
        status VARCHAR(20) DEFAULT 'ACTIVE'
    );

    CREATE TABLE IF NOT EXISTS odczyty (
        id SERIAL PRIMARY KEY,
        licznik_id INT REFERENCES liczniki(id),
        zuzycie_kwh NUMERIC(8,3),
        timestamp_odczytu TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    INSERT INTO taryfy (id, nazwa_taryfy, cena_dzien, cena_noc, godzina_noc_start, godzina_noc_koniec, znizka_weekend)
    VALUES 
    (1, 'G12 (Dwustrefowa)', 1.15, 0.45, '22:00:00', '06:00:00', FALSE),
    (2, 'G11 (Jednostrefowa)', 0.85, 0.85, '00:00:00', '00:00:00', FALSE),
    (3, 'G12W (Weekendowa)', 1.20, 0.40, '22:00:00', '06:00:00', TRUE),
    (4, 'C11 (Firma)', 1.35, 1.35, '00:00:00', '00:00:00', FALSE)
    ON CONFLICT (id) DO NOTHING;

    INSERT INTO klienci (pesel, imie, nazwisko, miasto)
    SELECT 
        LPAD((RANDOM() * 99999999999)::BIGINT::TEXT, 11, '0'),
        'Klient_' || g.id,
        'Nazwisko_' || g.id,
        (ARRAY['Gdańsk', 'Warszawa', 'Kraków', 'Wrocław', 'Poznań'])[floor(random() * 5) + 1]
    FROM generate_series(1, 10000) AS g(id)
    ON CONFLICT DO NOTHING;

    INSERT INTO liczniki (klient_id, taryfa_id, mac_address, model)
    SELECT 
        id,
        (FLOOR(RANDOM() * 4) + 1)::INT,
        SUBSTRING(MD5(RANDOM()::TEXT), 1, 17),
        (ARRAY['SmartMeter V1', 'SmartMeter V2', 'EcoReader Pro'])[floor(random() * 3) + 1]
    FROM klienci
    ON CONFLICT DO NOTHING;
    EOF
  }
}
resource "kubernetes_stateful_set" "postgres_source" {
  metadata {
    name      = "postgres-source"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  spec {
    service_name = "postgres-source"
    replicas     = 1
    selector {
      match_labels = {
        app = "postgres-source"
      }
    }
    template {
      metadata {
        labels = {
          app = "postgres-source"
        }
      }
      spec {
        volume {
          name = "init-script"
          config_map {
            name = kubernetes_config_map.postgres_source_init.metadata[0].name
          }
        }
        container {
          name  = "postgres"
          image = "postgres:16"

          args = ["-c", "wal_level=logical"]

          env {
            name  = "POSTGRES_USER"
            value = "admin"
          }
          env {
            name  = "POSTGRES_PASSWORD"
            value = var.db_password
          }
          env {
            name  = "POSTGRES_DB"
            value = "source_db"
          }
          port {
            container_port = 5432
          }
          
          volume_mount {
            name       = "postgres-data"
            mount_path = "/var/lib/postgresql/data"
          }
          volume_mount {
            name       = "init-script"
            mount_path = "/docker-entrypoint-initdb.d"
          }
        }
      }
    }
    
    volume_claim_template {
      metadata {
        name = "postgres-data"
      }
      spec {
        access_modes       = ["ReadWriteOnce"]
        storage_class_name = "standard"
        resources {
          requests = {
            storage = "5Gi"
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "postgres_source_svc" {
  metadata {
    name      = "postgres-source"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  spec {
    selector = {
      app = "postgres-source"

    }
    
    port {
      port        = 5432
      target_port = 5432
    }
  }
}

# Baza docelowa przechowuje wymiary i fakty
resource "kubernetes_stateful_set" "postgres_sink" {
  metadata {
    name      = "postgres-sink"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  spec {
    service_name = "postgres-sink"
    replicas     = 1
    selector {
      match_labels = {
        app = "postgres-sink"
      }
    }
    template {
      metadata {
        labels = {
          app = "postgres-sink"
        }
      }
      spec {
        volume {
          name = "init-script-sink"
          config_map {
            name = kubernetes_config_map.postgres_sink_init.metadata[0].name
          }
        }
        container {
          name  = "postgres"
          image = "postgres:16"
          
          env {
            name  = "POSTGRES_USER"
            value = "admin"
          }
          env {
            name  = "POSTGRES_PASSWORD"
            value = var.db_password
          }
          env {
            name  = "POSTGRES_DB"
            value = "sink_db"
          }
          port {
            container_port = 5432
          }
          
          
          volume_mount {
            name       = "postgres-data"
            mount_path = "/var/lib/postgresql/data"
          }
          volume_mount {
            name       = "init-script-sink"
            mount_path = "/docker-entrypoint-initdb.d"
          }
        }
      }
    }
    
    volume_claim_template {
      metadata {
        name = "postgres-data"
      }
      spec {
        access_modes       = ["ReadWriteOnce"]
        storage_class_name = "standard"
        resources {
          requests = {
            storage = "5Gi"
          }
        }
      }
    }
  }
}
resource "kubernetes_config_map" "postgres_sink_init" {
  metadata {
    name      = "postgres-sink-init"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }

  data = {
    "init.sql" = <<EOF
    CREATE TABLE IF NOT EXISTS dim_klient (
        klient_id INT PRIMARY KEY,
        miasto VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS dim_licznik (
        licznik_id INT PRIMARY KEY,
        model VARCHAR(50),
        taryfa_nazwa VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS log_bledow_etl (
        id SERIAL PRIMARY KEY,
        surowy_payload TEXT,
        powod_odrzucenia TEXT,
        timestamp_bledu TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE TABLE IF NOT EXISTS fakt_odczyty (
        id SERIAL PRIMARY KEY,
        licznik_id INT REFERENCES dim_licznik(licznik_id),
        klient_id INT REFERENCES dim_klient(klient_id),
        zuzycie_kwh NUMERIC(8,3),
        koszt_pln NUMERIC(8,2),
        is_anomalia BOOLEAN, -- Informacja o odczycie powyżej progu.
        godzina INT,
        czy_weekend BOOLEAN,
        timestamp_odczytu TIMESTAMP
    );
EOF
  }
}
resource "kubernetes_service" "postgres_sink_svc" {
  metadata {
    name      = "postgres-sink"
    namespace = kubernetes_namespace.dataops.metadata[0].name
  }
  
  spec {
    selector = {
      app = "postgres-sink"
    }
    port {
      port        = 5432
      target_port = 5432
    }
  }
}
  