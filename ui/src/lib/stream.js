/** SSE stream client with graceful poll fallback. */

/**
 * @typedef {{
 *   onTape?: (venue: string, symbol: string, entries: object[]) => void,
 *   onBook?: (venue: string, symbol: string, book: object) => void,
 *   onStatus?: (status: object) => void,
 *   onError?: (err: Error) => void,
 *   onConnect?: () => void,
 *   onDisconnect?: () => void,
 * }} StreamHandlers
 */

export class StreamClient {
  /**
   * @param {StreamHandlers} handlers
   */
  constructor(handlers = {}) {
    this.handlers = handlers;
    /** @type {EventSource|null} */
    this.es = null;
    this.connected = false;
    this.available = null; // null = unknown, true/false after probe
  }

  /**
   * Probe whether SSE endpoint exists.
   * @returns {Promise<boolean>}
   */
  async probe() {
    try {
      const res = await fetch('/v1/stream?probe=1', {
        method: 'HEAD',
        signal: AbortSignal.timeout(2000),
      });
      if (res.status === 404 || res.status === 405) {
        this.available = false;
        return false;
      }
      this.available = res.ok || res.status === 200;
      return this.available;
    } catch {
      // Try EventSource briefly
      return new Promise((resolve) => {
        let es;
        try {
          es = new EventSource('/v1/stream?probe=1');
          const t = setTimeout(() => {
            es.close();
            this.available = false;
            resolve(false);
          }, 1500);
          es.onopen = () => {
            clearTimeout(t);
            es.close();
            this.available = true;
            resolve(true);
          };
          es.onerror = () => {
            clearTimeout(t);
            es.close();
            this.available = false;
            resolve(false);
          };
        } catch {
          this.available = false;
          resolve(false);
        }
      });
    }
  }

  /**
   * @param {{ asset?: string, venues?: string[] }} opts
   */
  connect(opts = {}) {
    this.disconnect();
    const q = new URLSearchParams();
    if (opts.asset) q.set('asset', opts.asset);
    if (opts.venues?.length) q.set('venues', opts.venues.join(','));
    const url = `/v1/stream?${q}`;

    try {
      this.es = new EventSource(url);
    } catch (e) {
      this.handlers.onError?.(e instanceof Error ? e : new Error(String(e)));
      this.handlers.onDisconnect?.();
      return false;
    }

    this.es.onopen = () => {
      this.connected = true;
      this.available = true;
      this.handlers.onConnect?.();
    };

    this.es.onerror = () => {
      if (this.connected) {
        this.connected = false;
        this.handlers.onDisconnect?.();
      }
    };

    this.es.addEventListener('tape', (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        this.handlers.onTape?.(msg.venue, msg.symbol, msg.entries || []);
      } catch {
        /* ignore malformed */
      }
    });

    this.es.addEventListener('book', (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        this.handlers.onBook?.(msg.venue, msg.symbol, msg);
      } catch {
        /* ignore */
      }
    });

    this.es.addEventListener('status', (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        this.handlers.onStatus?.(msg);
      } catch {
        /* ignore */
      }
    });

    this.es.onmessage = (ev) => {
      try {
        const msg = JSON.parse(ev.data);
        if (msg.type === 'tape') {
          this.handlers.onTape?.(msg.venue, msg.symbol, msg.entries || []);
        } else if (msg.type === 'book') {
          this.handlers.onBook?.(msg.venue, msg.symbol, msg);
        } else if (msg.type === 'status') {
          this.handlers.onStatus?.(msg);
        }
      } catch {
        /* ignore */
      }
    };

    return true;
  }

  disconnect() {
    if (this.es) {
      this.es.close();
      this.es = null;
    }
    this.connected = false;
  }
}
