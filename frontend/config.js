// VAULT frontend connection settings.
// Blank values mean: use the current page's backend (recommended when
// the dashboard is served by FastAPI on http://127.0.0.1:8000).
// When using Vite on :5173, automatically use the FastAPI backend on :8000.
(function () {
  const host = window.location.hostname || '127.0.0.1';
  const protocol = window.location.protocol;
  const isViteDev = window.location.port === '5173';
  const api = isViteDev ? `${protocol}//${host}:8000` : '';
  const ws = isViteDev
    ? `${protocol === 'https:' ? 'wss' : 'ws'}://${host}:8000/ws`
    : '';

  window.VAULT_CONFIG = {
    API: api,
    WS: ws
  };
})();
