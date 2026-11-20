// What one client's application was handed, checked independently of the SDK's
// own dedupe. Numbers rise across epochs; only a void range (onLost) or a reset
// (onReset, the application reloads) lowers the expected next number.
export function deliveryOrder() {
  let high = null;
  return {
    /** True when the application was handed a number it already had: a repeat. */
    message(p) { const repeat = high !== null && p.n <= high; if (!repeat) high = p.n; return repeat; },
    lost(first) { if (high !== null) high = Math.min(high, first - 1); },
    reset(p) { high = p.n - 1; },
  };
}
// A position the SDK discarded is a catch-up overlap when a history page this
// client fetched carried it: the held live copy or the page's row arrived twice
// by design (ADR 0040, client rule 3). Any other discard is a live re-send.
export function historyCoverage() {
  let high = null;
  return {
    page(messages) { for (const m of messages) if (high === null || m.position.n > high) high = m.position.n; },
    covers(p) { return high !== null && p.n <= high; },
  };
}
