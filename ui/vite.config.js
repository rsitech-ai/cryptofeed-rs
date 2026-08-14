import { defineConfig } from 'vite';
import { svelte } from '@sveltejs/vite-plugin-svelte';

const uiProxy = process.env.MF_UI_PROXY || 'http://127.0.0.1:19109';
const telemetryProxy = process.env.MF_TELEMETRY_PROXY || 'http://127.0.0.1:19108';

export default defineConfig({
  plugins: [
    svelte(),
    {
      name: 'quiet-browser-icons',
      configureServer(server) {
        server.middlewares.use((req, res, next) => {
          const path = req.url?.split('?')[0] || '';
          if (
            path === '/favicon.ico' ||
            path === '/apple-touch-icon.png' ||
            path === '/apple-touch-icon-precomposed.png'
          ) {
            res.statusCode = 204;
            res.end();
            return;
          }
          next();
        });
      },
    },
  ],
  server: {
    port: 5173,
    proxy: {
      '/v1': uiProxy,
      '/live': telemetryProxy,
      '/ready': telemetryProxy,
      '/metrics': telemetryProxy,
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    cssCodeSplit: false,
    rollupOptions: {
      output: {
        // Single bundle so daemon embed (app.js + app.css only) stays valid.
        inlineDynamicImports: true,
        entryFileNames: 'assets/app.js',
        assetFileNames: (info) => {
          if (info.name && info.name.endsWith('.css')) return 'assets/app.css';
          return 'assets/[name][extname]';
        },
      },
    },
  },
});
