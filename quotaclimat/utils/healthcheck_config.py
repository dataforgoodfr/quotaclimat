
import http.server
import socketserver
import os
import logging
import asyncio
import tomli

def get_app_version():
     # Open and read the pyproject.toml file
    with open('pyproject.toml', 'rb') as toml_file:
        pyproject_data = tomli.load(toml_file)

    # Access the version from the pyproject.toml file
    version = pyproject_data['tool']['poetry']['version']
    return version

version = get_app_version()

class HealthCheckHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write((f"Healthy.\n\nApp version {version}").encode())

async def run_health_check_server():
    PORT = int(os.environ.get("HEALTHCHECK_PORT", 5000))
    SERVER_ADDRESS = os.environ.get("HEALTHCHECK_SERVER", "")

    logging.info(f"App version {version}")
    logging.info(f"Healthcheck at '{SERVER_ADDRESS}' : port {PORT}")
    with socketserver.TCPServer((SERVER_ADDRESS, PORT), HealthCheckHandler) as httpd:
        try:
            await asyncio.to_thread(httpd.serve_forever)
        except asyncio.CancelledError:
            logging.info("health check cancel")
            httpd.shutdown() # to terminal infinite loop "serve_forever"
            return
            