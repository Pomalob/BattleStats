import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import tailwindcss from '@tailwindcss/vite';

// The whole site is this app; FastAPI serves dist/ and answers /api.
// In development the API is proxied to the FastAPI server.
export default defineConfig({
  base: '/',
  plugins: [react(), tailwindcss()],
  server: {
    port: 5173,
    proxy: { '/api': 'http://127.0.0.1:8811' },
  },
  build: { outDir: 'dist', emptyOutDir: true },
});
