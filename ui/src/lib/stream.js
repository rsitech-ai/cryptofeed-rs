/** SSE stream client with graceful poll fallback. */

/**
 * @typedef {{
 *   onTape?: (venue: string, symbol: string, entries: object[]) => void,
 *   onBook?: (venue: string, symbol: string, book: object) => void,
 *   onStatus?: (status: object) => void,
 *   onFocus?: (focus: { venue: string, symbol: string, book?: object, tape?: object[] }) => void,
 *   onError?: (err: Error) => void,
 *   onConnect?: () => void,
 *   onDisconnect?: () => void,
 * }} StreamHandlers
 */

/**
 * Normalize a daemon SSE JSON frame into handler calls.
 * Server emits unnamed `data:` frames shaped as:
 *   `{ ts_ns, status, focus?: { venue, symbol, book, tape } }`
 * Older/typed shapes (`type` / named events) are still accepted.
 * @param {object} msg
 * @param {StreamHandlers} handlers
 */
export function dispatchStreamMessage(msg, handlers) {
  if (!msg || typeof msg !== 'object') return;

  if (msg.type === 'tape') {
    handlers.onTape?.(msg.venue, msg.symbol, msg.entries || []);
    return;
  }
  if (msg.type === 'book') {
    const book = msg.book && typeof msg.book === 'object' ? msg.book : msg;
    handlers.onBook?.(msg.venue, msg.symbol, book);
    return;
  }
  if (msg.type === 'status') {
    handlers.onStatus?.(msg.status || msg);
    return;
  }

  // Combined focus payload (primary daemon contract).
  if (msg.status) handlers.onStatus?.(msg.status);

  const focus = msg.focus;
  if (focus && typeof focus === 'object') {
    const venue = focus.venue || focus.book?.venue;
    const symbol = focus.symbol || focus.book?.symbol;
    if (venue && symbol) {
      handlers.onFocus?.({
        venue,
        symbol,
        book: focus.book || null,
        tape: Array.isArray(focus.tape) ? focus.tape : [],
      });
      if (focus.book) handlers.onBook?.(venue, symbol, focus.book);
      if (Array.isArray(focus.tape)) handlers.onTape?.(venue, symbol, focus.tape);
    }
  }
}

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
    /** Last focus apply time (ms) — SPA uses this to avoid double-skipping polls. */
    this.lastFocusAt = 0;
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
   * @param {{ asset?: string, venue?: string, symbol?: string, venues?: string[] }} opts
   */
  connect(opts = {}) {
    this.disconnect();
    const q = new URLSearchParams();
    // Prefer explicit focus venue/symbol so SSE tracks the selected market,
    // not just the first asset match in daemon config order.
    if (opts.venue && opts.symbol) {
      q.set('venue', opts.venue);
      q.set('symbol', opts.symbol);
    } else if (opts.asset) {
      q.set('asset', opts.asset);
    }
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

    const handle = (raw) => {
      try {
        const msg = JSON.parse(raw);
        const before = this.lastFocusAt;
        const wrapped = {
          ...this.handlers,
          onFocus: (f) => {
            this.lastFocusAt = Date.now();
            this.handlers.onFocus?.(f);
          },
          onBook: (v, s, b) => {
            // Book-only events also count as focus freshness when matching.
            this.lastFocusAt = Date.now();
            this.handlers.onBook?.(v, s, b);
          },
          onTape: (v, s, e) => {
            this.lastFocusAt = Date.now();
            this.handlers.onTape?.(v, s, e);
          },
        };
        dispatchStreamMessage(msg, wrapped);
        // If payload had no focus, still allow status-only updates without bumping focus.
        if (this.lastFocusAt === before && msg?.focus) this.lastFocusAt = Date.now();
      } catch {
        /* ignore malformed */
      }
    };

    this.es.addEventListener('tape', (ev) => handle(ev.data));
    this.es.addEventListener('book', (ev) => handle(ev.data));
    this.es.addEventListener('status', (ev) => handle(ev.data));
    this.es.onmessage = (ev) => handle(ev.data);

    return true;
  }

  /**
   * True when SSE delivered focus book/tape recently.
   * @param {number} [maxAgeMs]
   */
  focusFresh(maxAgeMs = 1500) {
    return this.connected && this.lastFocusAt > 0 && Date.now() - this.lastFocusAt < maxAgeMs;
  }

  disconnect() {
    if (this.es) {
      this.es.close();
      this.es = null;
    }
    this.connected = false;
  }
}
