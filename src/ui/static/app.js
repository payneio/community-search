(() => {
  const form = document.getElementById("search-form");
  const qInput = document.getElementById("q");
  const collSelect = document.getElementById("collection");
  const collMeta = document.getElementById("collection-meta");
  const results = document.getElementById("results");
  const status = document.getElementById("status");

  // name → document count, populated by loadCollections, consumed by
  // updateCollectionMeta on dropdown change.
  const collectionDocCounts = new Map();

  function updateCollectionMeta() {
    const name = collSelect.value;
    if (!name) {
      collMeta.textContent = "";
      return;
    }
    const n = collectionDocCounts.get(name);
    if (n == null) {
      collMeta.textContent = "";
      return;
    }
    collMeta.textContent = n.toLocaleString() + " page" + (n === 1 ? "" : "s") + " in " + name;
  }

  async function loadCollections() {
    try {
      const r = await fetch("/api/collections");
      const j = await r.json();
      for (const c of j.collections || []) {
        const opt = document.createElement("option");
        opt.value = c.name;
        opt.textContent = c.name;
        collSelect.appendChild(opt);
        if (typeof c.documents === "number") {
          collectionDocCounts.set(c.name, c.documents);
        }
      }
      updateCollectionMeta();
    } catch (e) {
      console.warn("collections load failed", e);
    }
  }

  collSelect.addEventListener("change", updateCollectionMeta);

  function escapeText(s) {
    const d = document.createElement("div");
    d.textContent = s;
    return d.innerHTML;
  }

  function relAge(ts) {
    const seconds = Math.floor(Date.now() / 1000) - ts;
    if (seconds < 60) return seconds + "s ago";
    if (seconds < 3600) return Math.floor(seconds / 60) + "m ago";
    if (seconds < 86400) return Math.floor(seconds / 3600) + "h ago";
    return Math.floor(seconds / 86400) + "d ago";
  }

  function appendResult(r) {
    const li = document.createElement("li");
    li.innerHTML = `
      <div class="result-title"><a href="${escapeText(r.url)}" target="_blank" rel="noopener noreferrer">${escapeText(r.title || r.url)}</a></div>
      <div class="result-url">${escapeText(r.url)}</div>
      <div class="result-snippet">${r.snippet_html || ""}</div>
      <div class="result-meta">${escapeText(r.source)} &middot; ${relAge(r.timestamp)}</div>
    `;
    results.appendChild(li);
  }

  async function runSearch(query, collection) {
    results.innerHTML = "";
    status.textContent = "Searching\u2026";
    const body = { query, collection: collection || null, remaining_depth: 1 };

    const res = await fetch("/api/search", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
    if (!res.ok || !res.body) {
      status.textContent = "Search failed (" + res.status + ")";
      return;
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = "";

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });

      let idx;
      while ((idx = buf.indexOf("\n\n")) !== -1) {
        const raw = buf.slice(0, idx);
        buf = buf.slice(idx + 2);
        const event = parseSse(raw);
        if (!event) continue;
        if (event.event === "result") {
          try { appendResult(JSON.parse(event.data)); } catch {}
        } else if (event.event === "done") {
          status.textContent = "Done.";
          return;
        } else if (event.event === "source_complete") {
          status.textContent = "Got results from " + (JSON.parse(event.data).source || "?") + "\u2026";
        }
      }
    }
  }

  function parseSse(raw) {
    const lines = raw.split("\n");
    const out = { event: "message", data: "" };
    for (const line of lines) {
      if (line.startsWith("event:")) out.event = line.slice(6).trim();
      else if (line.startsWith("data:")) out.data += line.slice(5).trim();
    }
    return out;
  }

  form.addEventListener("submit", (e) => {
    e.preventDefault();
    runSearch(qInput.value.trim(), collSelect.value).catch((err) => {
      status.textContent = "Error: " + err.message;
    });
  });

  loadCollections();
})();
