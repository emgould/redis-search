/**
 * Search Controller — Two-tier debounce search with streaming support.
 *
 * Architecture:
 *   Tier 1 — Autocomplete (300ms debounce): hits /api/autocomplete for fast
 *            local RediSearch results (tv, movie, person, podcast, author, book).
 *   Tier 2 — Full Search (750ms debounce or Enter): hits /api/search (batch)
 *            or /api/search/stream (SSE) for all sources including brokered APIs
 *            (news, video, ratings, artist, album).
 *
 * Both tiers merge into a shared `currentResults` object.  Autocomplete always
 * overwrites (it is the first responder).  Search always overwrites (it is
 * authoritative for every source, including brokered).
 *
 * Cancellation: when the query text changes, all in-flight requests and streams
 * are cancelled and currentResults is reset.  Debounce timers are cleared on
 * every keystroke.
 *
 * Usage:
 *   const controller = initSearchController({ ... });
 *   // controller.searchFor("The Office");
 *   // controller.cancelAll();
 */

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const AUTOCOMPLETE_DEBOUNCE_MS = 300;
const SEARCH_DEBOUNCE_MS = 750;

// ---------------------------------------------------------------------------
// Public init
// ---------------------------------------------------------------------------

/**
 * @param {Object} cfg
 * @param {HTMLInputElement}  cfg.searchInput
 * @param {HTMLElement}       cfg.resultsContainer
 * @param {HTMLElement}       cfg.loadingSpinner
 * @param {function():string|null} cfg.getActiveFiltersParam  — returns comma-separated filter string or null
 * @param {function(string):boolean} cfg.isRawMode            — true when query contains @
 * @param {function(string):string}  cfg.getApiUrl            — prepends API base to path
 * @param {function(Object,string):void} cfg.renderResults    — renders {tv:[], movie:[], …} + query
 * @param {Object}            cfg.expandedCategories          — mutable object tracking expand/collapse state
 * @param {function():boolean} cfg.isStreamingEnabled         — returns current stream toggle state
 */
function initSearchController(cfg) {
  const {
    searchInput,
    resultsContainer,
    loadingSpinner,
    getActiveFiltersParam,
    isRawMode,
    getApiUrl,
    renderResults,
    expandedCategories,
    isStreamingEnabled,
  } = cfg;

  // ---- State ----
  let autocompleteTimer = null;
  let searchTimer = null;
  let autocompleteCtrl = null; // AbortController for autocomplete fetch
  let searchCtrl = null;       // AbortController for batch search fetch
  let eventSource = null;      // EventSource for stream search
  let currentQuery = "";
  let currentResults = {};

  // ---- Helpers ----

  function buildUrl(endpoint, query, extraParams) {
    const sources = getActiveFiltersParam();
    const raw = isRawMode(query) ? "&raw=true" : "";
    let url = getApiUrl(
      `${endpoint}?q=${encodeURIComponent(query)}${raw}`
    );
    if (sources) url += `&sources=${encodeURIComponent(sources)}`;
    if (extraParams) {
      for (const [k, v] of Object.entries(extraParams)) {
        url += `&${k}=${encodeURIComponent(v)}`;
      }
    }
    return url;
  }

  function showSpinner() {
    loadingSpinner.classList.remove("hidden");
  }
  function hideSpinner() {
    loadingSpinner.classList.add("hidden");
  }
  function showResults() {
    resultsContainer.classList.remove("hidden");
  }
  function hideResults() {
    resultsContainer.classList.add("hidden");
  }

  function cancelAll() {
    if (autocompleteCtrl) { autocompleteCtrl.abort(); autocompleteCtrl = null; }
    if (searchCtrl) { searchCtrl.abort(); searchCtrl = null; }
    if (eventSource) { eventSource.close(); eventSource = null; }
  }

  function clearTimers() {
    clearTimeout(autocompleteTimer);
    clearTimeout(searchTimer);
  }

  function resetExpandedState() {
    for (const key of Object.keys(expandedCategories)) {
      expandedCategories[key] = false;
    }
  }

  function render(query) {
    renderResults(currentResults, query);
    showResults();
  }

  // ---- Tier 1: Autocomplete ----

  async function runAutocomplete(query) {
    if (autocompleteCtrl) autocompleteCtrl.abort();
    autocompleteCtrl = new AbortController();
    showSpinner();

    try {
      const url = buildUrl("/api/autocomplete", query);
      const resp = await fetch(url, { signal: autocompleteCtrl.signal });
      const data = await resp.json();

      if (query === currentQuery) {
        currentResults = { ...currentResults, ...data };
        render(query);
      }
    } catch (e) {
      if (e.name === "AbortError") return;
      console.error("[Autocomplete] Error:", e);
    } finally {
      hideSpinner();
    }
  }

  // ---- Tier 2a: Batch search ----

  async function runSearchBatch(query) {
    if (searchCtrl) searchCtrl.abort();
    searchCtrl = new AbortController();
    showSpinner();

    try {
      const url = buildUrl("/api/search", query, { limit: "10" });
      console.log("[Search Batch]", url);
      const resp = await fetch(url, { signal: searchCtrl.signal });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();

      if (query === currentQuery) {
        console.log(
          "[Search Batch] results:",
          Object.keys(data)
            .map((k) => `${k}:${data[k]?.length || 0}`)
            .join(", ")
        );
        currentResults = { ...currentResults, ...data };
        render(query);
      }
    } catch (e) {
      if (e.name === "AbortError") return;
      console.error("[Search Batch] Error:", e);
    } finally {
      hideSpinner();
    }
  }

  // ---- Tier 2b: Stream search ----

  function runSearchStream(query) {
    if (eventSource) { eventSource.close(); eventSource = null; }
    showSpinner();

    const url = buildUrl("/api/search/stream", query, { limit: "10" });
    console.log("[Search Stream]", url);
    eventSource = new EventSource(url);
    const startTime = performance.now();
    let sourcesReceived = 0;

    eventSource.addEventListener("result", (e) => {
      if (query !== currentQuery) return;
      try {
        const { source, results, latency_ms } = JSON.parse(e.data);
        sourcesReceived++;
        currentResults[source] = results;
        render(query);
        console.debug(
          `[Search Stream] ${source}: ${results.length} results in ${latency_ms}ms (${sourcesReceived} sources)`
        );
      } catch (err) {
        console.error("[Search Stream] parse error:", err);
      }
    });

    eventSource.addEventListener("exact_match", (e) => {
      if (query !== currentQuery) return;
      try {
        const item = JSON.parse(e.data);
        // First exact match wins (matches batch behavior: single best by priority)
        if (!currentResults.exact_match) {
          currentResults.exact_match = item;
          render(query);
        }
        console.debug("[Search Stream] exact_match:", item.search_title || item.name || item.title);
      } catch (err) {
        console.error("[Search Stream] exact_match parse error:", err);
      }
    });

    eventSource.addEventListener("done", () => {
      console.log(
        `[Search Stream] done: ${sourcesReceived} sources in ${Math.round(performance.now() - startTime)}ms`
      );
      hideSpinner();
      eventSource.close();
      eventSource = null;
    });

    eventSource.addEventListener("error", () => {
      hideSpinner();
      if (eventSource) { eventSource.close(); eventSource = null; }
    });
  }

  // ---- Dispatch ----

  function triggerSearch(query) {
    if (isStreamingEnabled()) {
      runSearchStream(query);
    } else {
      runSearchBatch(query);
    }
  }

  // ---- Event wiring ----

  searchInput.addEventListener("input", (e) => {
    const query = e.target.value.trim();

    // When query changes, cancel everything and reset accumulated results
    if (query !== currentQuery) {
      cancelAll();
      clearTimers();
      currentResults = {};
      resetExpandedState();
    } else {
      clearTimers();
    }

    currentQuery = query;

    if (!query || query.length < 2) {
      hideResults();
      return;
    }

    // Tier 1: autocomplete after short pause
    autocompleteTimer = setTimeout(() => runAutocomplete(query), AUTOCOMPLETE_DEBOUNCE_MS);

    // Tier 2: full search after longer pause
    searchTimer = setTimeout(() => triggerSearch(query), SEARCH_DEBOUNCE_MS);
  });

  searchInput.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      hideResults();
      searchInput.blur();
      return;
    }
    if (e.key === "Enter") {
      e.preventDefault();
      const query = searchInput.value.trim();
      if (query.length < 2) return;
      clearTimers();
      currentQuery = query;
      currentResults = {};
      resetExpandedState();
      triggerSearch(query);
    }
  });

  searchInput.addEventListener("focus", () => {
    const query = searchInput.value.trim();
    if (query.length < 2) return;
    if (Object.keys(currentResults).length > 0 && currentQuery === query) {
      render(query);
    } else {
      currentQuery = query;
      runAutocomplete(query);
    }
  });

  // Close results when clicking outside the search container
  document.addEventListener("click", (e) => {
    if (!searchInput.closest("#search-container")?.contains(e.target)) {
      hideResults();
    }
  });

  // ---- Public API ----

  return {
    searchFor(query) {
      searchInput.value = query;
      searchInput.dispatchEvent(new Event("input"));
      searchInput.focus();
    },
    cancelAll,
    getCurrentQuery() { return currentQuery; },
    getCurrentResults() { return currentResults; },
    triggerSearch(query) {
      currentQuery = query;
      triggerSearch(query);
    },
  };
}
