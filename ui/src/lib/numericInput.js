/**
 * Parse and clamp a numeric input draft without treating an empty field as zero.
 * @param {string | number} raw
 * @param {{ min?: number, max?: number, integer?: boolean }} [options]
 * @returns {number | null}
 */
export function normalizeNumericDraft(raw, options = {}) {
  if (String(raw).trim() === '') return null;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return null;

  const min = Number.isFinite(options.min) ? Number(options.min) : -Infinity;
  const max = Number.isFinite(options.max) ? Number(options.max) : Infinity;
  const normalized = options.integer ? Math.round(parsed) : parsed;
  return Math.min(max, Math.max(min, normalized));
}

/**
 * Svelte action for numeric fields that must retain an in-progress edit while
 * live parent props continue to update. The draft commits on blur or Enter.
 *
 * @param {HTMLInputElement} node
 * @param {{ value: number, min?: number, max?: number, integer?: boolean, onCommit: (value: number) => void }} initial
 */
export function numericCommit(node, initial) {
  let params = initial;
  let editing = false;
  let lastEmitted = normalizeNumericDraft(initial.value, initial);

  const provenValue = () => {
    const value = normalizeNumericDraft(params.value, params);
    return value == null ? '' : String(value);
  };

  const sync = () => {
    if (!editing) node.value = provenValue();
  };

  const commit = () => {
    const value = normalizeNumericDraft(node.value, params);
    if (value == null) {
      node.value = provenValue();
      return;
    }
    node.value = String(value);
    if (value === lastEmitted) return;
    lastEmitted = value;
    params.onCommit(value);
  };

  const onFocus = () => { editing = true; };
  const onBlur = () => {
    commit();
    editing = false;
  };
  const onKeydown = (event) => {
    if (event.key !== 'Enter') return;
    event.preventDefault();
    commit();
    node.blur();
  };

  node.addEventListener('focus', onFocus);
  node.addEventListener('blur', onBlur);
  node.addEventListener('keydown', onKeydown);
  sync();

  return {
    update(next) {
      params = next;
      const external = normalizeNumericDraft(next.value, next);
      if (external != null) lastEmitted = external;
      sync();
    },
    destroy() {
      node.removeEventListener('focus', onFocus);
      node.removeEventListener('blur', onBlur);
      node.removeEventListener('keydown', onKeydown);
    },
  };
}
