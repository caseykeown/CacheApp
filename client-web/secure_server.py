import http.server
import ssl
import os
import sys

# Locate the tailscale certs in the same directory as this script
script_dir = os.path.dirname(os.path.abspath(__file__))
cert_file = os.path.join(script_dir, "caseys-mintbook.tail09bf4f.ts.net.crt")
key_file = os.path.join(script_dir, "caseys-mintbook.tail09bf4f.ts.net.key")

# Fallback check to current working directory
if not os.path.exists(cert_file) or not os.path.exists(key_file):
    cert_file = "caseys-mintbook.tail09bf4f.ts.net.crt"
    key_file = "caseys-mintbook.tail09bf4f.ts.net.key"

if not os.path.exists(cert_file) or not os.path.exists(key_file):
    print("Error: Tailscale SSL certificate files not found!")
    print("Please run the following command in your terminal first:")
    print("  sudo tailscale cert caseys-mintbook.tail09bf4f.ts.net")
    sys.exit(1)

# Navigate to the public assets directory containing index.html to serve it securely
public_dir = os.path.join(script_dir, "public")
if not os.path.exists(public_dir):
    print(f"Error: Public folder not found at {public_dir}")
    sys.exit(1)

os.chdir(public_dir)

server_address = ('0.0.0.0', 8443)
httpd = http.server.HTTPServer(server_address, http.server.SimpleHTTPRequestHandler)

# Bind the native Python SSL context
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain(certfile=cert_file, keyfile=key_file)
httpd.socket = context.wrap_socket(httpd.socket, server_side=True)

print("------------------------------------------------------------")
print("🔒 SECURE WEB CAPTURE SERVER ACTIVE")
print("👉 Open on your iPhone: https://caseys-mintbook.tail09bf4f.ts.net:8443")
print("------------------------------------------------------------")

try:
    httpd.serve_forever()
except KeyboardInterrupt:
    print("\nShutting down Secure Server.")
    httpd.server_close()

