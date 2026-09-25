# Optional no-domain HTTPS for a demo

If you do not have a domain, a Cloudflare Tunnel can expose the local coordinator over HTTPS. Install `cloudflared` on the Oracle VM and authenticate it with a Cloudflare account.

For a temporary demo tunnel:

```bash
cloudflared tunnel --url http://127.0.0.1:8000
```

Use the generated HTTPS URL as:

```text
VITE_API_URL=https://<generated-host>
VITE_WS_URL=wss://<generated-host>/ws
```

For a permanent demo URL, create a named tunnel and map a hostname such as `api.yourdomain.com` to `http://127.0.0.1:8000`.
