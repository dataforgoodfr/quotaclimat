"""Minimal Prometheus /metrics endpoint (Partie 2 de la consigne).

Expose les métriques par défaut de prometheus_client (process CPU,
mémoire, garbage collector) sur le port 8000, plus un compteur
"heartbeat" qui augmente toutes les 5 secondes pour avoir au moins une
métrique applicative à visualiser dans Grafana.
"""
import time

from prometheus_client import Counter, start_http_server

heartbeat = Counter("quotaclimat_heartbeat_total", "Nombre de battements depuis le démarrage")

if __name__ == "__main__":
    start_http_server(8000)
    while True:
        heartbeat.inc()
        time.sleep(5)
