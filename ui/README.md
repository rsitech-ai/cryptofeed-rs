# Marketfeed UI

Dense loopback market panel for the daemon view API.

Checked-in `dist/` assets are served by `--features ui` (no Node required to run).
Optional Svelte sources under `src/` can rebuild `dist/` when Node is available:

```bash
npm install
npm run build   # overwrites dist/
npm run dev     # proxies to offline binds 19108/19109
```

See [`docs/ops/ui.md`](../docs/ops/ui.md).
