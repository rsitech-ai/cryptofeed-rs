/** SSE stream client with graceful poll fallback. */

/**
 * @typedef {{
 *   onTape?: (venue: string, symbol: string, entries: object[]) => void,
 *   onBook?: (venue: string, symbol: string, book: object) => void,
 *   onStatus?: (status: object) => void,
 *   onFocus?: (focus: { venue: string, symbol: string, book?: object, tape?: object[], profile?: object, structuralLevels?: object }) => void,
 *   onError?: (err: Error) => void,
 *   onConnect?: () => void,
 *   onDisconnect?: () => void,
 *   onReconnecting?: () => void,
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
  // Call onFocus once — do NOT also fire onBook/onTape (that double-applies and
  // causes UI flicker when the SPA handlers already apply book+tape in onFocus).
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
        profile: focus.profile && typeof focus.profile === 'object' ? focus.profile : null,
        bubblesVolume: focus.bubbles_volume && typeof focus.bubbles_volume === 'object' ? focus.bubbles_volume : null,
        bubblesDelta: focus.bubbles_delta && typeof focus.bubbles_delta === 'object' ? focus.bubbles_delta : null,
        structuralLevels: focus.structural_levels && typeof focus.structural_levels === 'object' ? focus.structural_levels : null,
        derivatives: focus.derivatives && typeof focus.derivatives === 'object' ? focus.derivatives : null,
      });
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
    /** Intentional teardown — suppress disconnect/reconnect chip flip. */
    this._silentClose = false;
    this.reconnectCount = 0;
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
    this.disconnect({ silent: true });
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
      const was = this.connected;
      this.connected = true;
      this.available = true;
      if (was) this.reconnectCount += 1;
      // Also notify after an EventSource auto-reconnect. Consumers use this
      // callback to clear their soft "reconnecting" indicator.
      this.handlers.onConnect?.();
    };

    this.es.onerror = () => {
      // EventSource auto-reconnects while CONNECTING. Only tear the UX down when
      // the socket is permanently CLOSED (or we intentionally closed it).
      if (this._silentClose) return;
      const state = this.es?.readyState;
      if (state === EventSource.CLOSED) {
        if (this.connected) {
          this.connected = false;
          this.handlers.onDisconnect?.();
        }
        return;
      }
      // Transient blip — keep streamMode as SSE; optional soft signal.
      this.handlers.onReconnecting?.();
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
    // Keep treating focus as fresh during brief EventSource CONNECTING blips so
    // poll fallback does not race and clear panels.
    const esOk =
      this.es &&
      (this.es.readyState === EventSource.OPEN || this.es.readyState === EventSource.CONNECTING);
    return (
      (!!this.connected || !!esOk) &&
      this.lastFocusAt > 0 &&
      Date.now() - this.lastFocusAt < maxAgeMs
    );
  }

  /**
   * @param {{ silent?: boolean }} [opts]
   */
  disconnect(opts = {}) {
    const silent = !!opts.silent;
    this._silentClose = silent;
    if (this.es) {
      try {
        this.es.onerror = null;
        this.es.onopen = null;
        this.es.onmessage = null;
        this.es.close();
      } catch {
        /* ignore */
      }
      this.es = null;
    }
    const was = this.connected;
    this.connected = false;
    this._silentClose = false;
    if (was && !silent) this.handlers.onDisconnect?.();
  }
}
