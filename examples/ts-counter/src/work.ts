// A fixed amount of plain JavaScript work, for comparing the two engines
// (`bench/compare`, `js_work_*` rows): the counter itself spends almost
// no time in JavaScript. Typical request handling: build records, JSON
// round-trip them, filter, sort, format strings, match a regex, hash.
// Deterministic, so both engines do exactly the same work; the result is a
// checksum so nothing can be skipped.

const WORDS = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"];

export function work(rounds = 4): string {
  let check = 0x811c9dc5;
  for (let r = 0; r < rounds; r++) {
    const records = [];
    for (let i = 0; i < 200; i++) {
      records.push({
        id: i,
        name: `${WORDS[(i * 7 + r) % WORDS.length]}-${i}`,
        email: `user${i}@example.com`,
        score: (i * 37 + r * 11) % 1000,
        tags: [WORDS[i % 8], WORDS[(i + 3) % 8]],
      });
    }
    const text = JSON.stringify(records);
    const parsed: typeof records = JSON.parse(text);
    const top = parsed
      .filter((p) => p.score > 300)
      .sort((a, b) => b.score - a.score || a.name.localeCompare(b.name))
      .slice(0, 50)
      .map((p) => `${p.name.toUpperCase()}:${p.score}:${p.tags.join("+")}`)
      .join(",");
    const emails = text.match(/user\d+@example\.com/g)?.length ?? 0;
    for (let i = 0; i < top.length; i++) {
      check = Math.imul(check ^ top.charCodeAt(i), 0x01000193) >>> 0;
    }
    check = (check + emails + text.length) >>> 0;
  }
  return check.toString(16);
}
