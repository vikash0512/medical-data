const crawlForm = document.querySelector("#crawl-form");
const websiteUrlsInput = document.querySelector("#website-urls");
const maxPagesInput = document.querySelector("#max-pages");
const maxDepthInput = document.querySelector("#max-depth");
const concurrencyInput = document.querySelector("#concurrency");
const includeSitemapInput = document.querySelector("#include-sitemap");
const crawlStatus = document.querySelector("#crawl-status");
const jobStatusPill = document.querySelector("#job-status-pill");
const statDiscovered = document.querySelector("#stat-discovered");
const statScraped = document.querySelector("#stat-scraped");
const statAccepted = document.querySelector("#stat-accepted");
const statRejected = document.querySelector("#stat-rejected");
const diagSource = document.querySelector("#diag-source");
const diagCpu = document.querySelector("#diag-cpu");
const diagRam = document.querySelector("#diag-ram");
const diagCpuValue = document.querySelector("#diag-cpu-value");
const diagRamValue = document.querySelector("#diag-ram-value");
const diagProcessRam = document.querySelector("#diag-process-ram");
const diagThreads = document.querySelector("#diag-threads");
const diagLoad1m = document.querySelector("#diag-load-1m");
const diagLoad5m = document.querySelector("#diag-load-5m");
const crawlProgress = document.querySelector("#crawl-progress");
const platformProgressList = document.querySelector("#platform-progress-list");
const crawlPause = document.querySelector("#crawl-pause");
const crawlResume = document.querySelector("#crawl-resume");
const crawlCancel = document.querySelector("#crawl-cancel");
const crawlReset = document.querySelector("#crawl-reset");
const datasetPreview = document.querySelector("#dataset-preview");
const downloadJson = document.querySelector("#download-json");
const downloadJsonl = document.querySelector("#download-jsonl");
const downloadCsv = document.querySelector("#download-csv");
const downloadPreview = document.querySelector("#download-preview");
const crawlNotes = document.querySelector("#crawl-notes");
const errorBox = document.querySelector("#error-box");
const mergeForm = document.querySelector("#merge-form");
const mergeSourceFiles = document.querySelector("#merge-source-files");
const mergeSubmit = document.querySelector("#merge-submit");
const mergeProgress = document.querySelector("#merge-progress");
const mergeDownload = document.querySelector("#merge-download");
const mergeStatus = document.querySelector("#merge-status");
const fileForm = document.querySelector("#file-form");
const fileInput = document.querySelector("#source-file");

const ACTIVE_CRAWL_KEY = "medical-extractor.activeCrawlJobId";

let mergedDownloadState = null;
let crawlNoteLines = [];
let latestStatusFingerprint = "";
let seenErrorNotes = new Set();
let seenInfoNotes = new Set();

let pollTimer = null;
let metricsTimer = null;
let activeCrawlJobId = null;
let jobPollIntervalMs = 3000;
let resourceWarningTimestamp = 0;

function setStatus(message, isError = false) {
  crawlStatus.textContent = message;
  crawlStatus.classList.toggle("error", isError);
}

function setPill(status) {
  jobStatusPill.textContent = status;
  jobStatusPill.className = `status-pill ${status}`;
}

function restartJobPolling(jobId, runImmediately = false) {
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }

  if (!jobId) {
    return;
  }

  pollTimer = setInterval(() => pollJob(jobId), jobPollIntervalMs);
  if (runImmediately) {
    void pollJob(jobId);
  }
}

function resetCrawlUiState() {
  setActiveCrawlJob(null);
  resetDownloads();
  renderPlatformProgress([]);
  datasetPreview.value = "[]";
  statDiscovered.textContent = "0";
  statScraped.textContent = "0";
  statAccepted.textContent = "0";
  statRejected.textContent = "0";
  crawlProgress.max = 1;
  crawlProgress.value = 0;
  crawlPause.disabled = true;
  crawlResume.disabled = true;
  crawlCancel.disabled = true;
}

async function readResponsePayload(response) {
  const contentType = response.headers.get("content-type") || "";
  if (contentType.includes("application/json")) {
    try {
      return await response.json();
    } catch {
      return {};
    }
  }

  try {
    const text = await response.text();
    return { detail: text };
  } catch {
    return {};
  }
}

async function postResetRequest(pathname) {
  const response = await fetch(pathname, { method: "POST" });
  const payload = await readResponsePayload(response);
  return { response, payload };
}

function formatNumber(value, fallback = "n/a") {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return fallback;
  }
  return `${Number(value).toFixed(1)}`;
}

function renderMetrics(metrics) {
  const cpu = Number(metrics.cpu_percent ?? 0);
  const ram = Number(metrics.memory_percent ?? 0);

  if (diagCpu) {
    diagCpu.value = Math.max(0, Math.min(100, cpu));
  }
  if (diagRam) {
    diagRam.value = Math.max(0, Math.min(100, ram));
  }
  if (diagCpuValue) {
    diagCpuValue.textContent = `${formatNumber(cpu, "0.0")}%`;
  }
  if (diagRamValue) {
    if (metrics.memory_percent === null || metrics.memory_percent === undefined) {
      diagRamValue.textContent = "n/a";
    } else {
      const used = metrics.memory_used_mb;
      const total = metrics.memory_total_mb;
      if (used !== null && used !== undefined && total !== null && total !== undefined) {
        diagRamValue.textContent = `${formatNumber(ram, "0.0")}% (${Math.round(used)}MB/${Math.round(total)}MB)`;
      } else {
        diagRamValue.textContent = `${formatNumber(ram, "0.0")}%`;
      }
    }
  }
  if (diagProcessRam) {
    diagProcessRam.textContent =
      metrics.process_memory_mb !== null && metrics.process_memory_mb !== undefined
        ? `${Math.round(Number(metrics.process_memory_mb))} MB`
        : "n/a";
  }
  if (diagThreads) {
    diagThreads.textContent =
      metrics.process_threads !== null && metrics.process_threads !== undefined
        ? `${metrics.process_threads}`
        : "n/a";
  }
  if (diagLoad1m) {
    diagLoad1m.textContent = metrics.load_average?.["1m"] ?? "n/a";
  }
  if (diagLoad5m) {
    diagLoad5m.textContent = metrics.load_average?.["5m"] ?? "n/a";
  }
  if (diagSource) {
    diagSource.textContent = metrics.psutil_available
      ? "system + process metrics"
      : "process fallback";
  }

  const processRamMb = Number(metrics.process_memory_mb ?? 0);
  const highCpu = cpu >= 85;
  const highProcessRam = processRamMb >= 1024;
  const nextPollInterval = highCpu || highProcessRam ? 6000 : 3000;

  if (nextPollInterval !== jobPollIntervalMs) {
    jobPollIntervalMs = nextPollInterval;
    if (activeCrawlJobId) {
      restartJobPolling(activeCrawlJobId, false);
    }
  }

  if (highCpu || highProcessRam) {
    const now = Date.now();
    if (now - resourceWarningTimestamp > 30000) {
      resourceWarningTimestamp = now;
      pushCrawlNote(
        `Resource guard enabled. CPU=${formatNumber(cpu, "0.0")}% ProcessRAM=${Math.round(processRamMb)}MB. Polling slowed to reduce load.`,
        "WARN"
      );
    }
  }
}

async function pollMetrics() {
  try {
    const response = await fetch("/api/system/metrics");
    const metrics = await response.json();
    if (!response.ok) {
      throw new Error(metrics.detail || "Unable to fetch system metrics.");
    }
    renderMetrics(metrics);
  } catch (error) {
    if (diagSource) {
      diagSource.textContent = "metrics unavailable";
    }
  }
}

function asNumber(input, fallback) {
  const value = Number.parseInt(input.value, 10);
  return Number.isFinite(value) ? value : fallback;
}

function setDownloadLink(anchor, url) {
  if (!url) {
    anchor.removeAttribute("href");
    anchor.removeAttribute("download");
    anchor.classList.add("disabled");
    anchor.setAttribute("aria-disabled", "true");
    return;
  }

  anchor.href = url;
  anchor.download = "";
  anchor.classList.remove("disabled");
  anchor.setAttribute("aria-disabled", "false");
}

function resetDownloads() {
  setDownloadLink(downloadJson, null);
  setDownloadLink(downloadJsonl, null);
  setDownloadLink(downloadCsv, null);
  downloadPreview.disabled = true;
}

function renderPlatformProgress(platforms = []) {
  if (!platformProgressList) {
    return;
  }

  if (!platforms.length) {
    platformProgressList.innerHTML = '<p class="platform-empty">No platform progress yet.</p>';
    return;
  }

  platformProgressList.innerHTML = platforms
    .map((platform) => {
      const processed = Math.max(platform.scraped_pages || 0, platform.accepted_pages || 0, platform.rejected_pages || 0);
      const percent = platform.max_pages > 0 ? Math.min(100, Math.round((processed / platform.max_pages) * 100)) : 0;
      return `
        <article class="platform-progress-card">
          <div class="platform-progress-card__top">
            <div>
              <h4>${escapeHtml(platform.label || platform.start_url)}</h4>
              <p>${escapeHtml(platform.start_url)}</p>
            </div>
            <span class="status-pill ${escapeHtml(platform.status || "queued")}">${escapeHtml(platform.status || "queued")}</span>
          </div>
          <progress value="${percent}" max="100"></progress>
          <div class="platform-progress-meta">
            <span>Scraped ${platform.scraped_pages || 0}/${platform.max_pages || 0}</span>
            <span>Accepted ${platform.accepted_pages || 0}</span>
            <span>Rejected ${platform.rejected_pages || 0}</span>
          </div>
          <p class="platform-progress-message">${escapeHtml(platform.status_message || "Queued.")}</p>
        </article>
      `;
    })
    .join("");
}

function nowStamp() {
  return new Date().toLocaleTimeString([], { hour12: false });
}

function pushCrawlNote(message, level = "INFO") {
  const line = `[${nowStamp()}] ${level}: ${message}`;
  const lastLine = crawlNoteLines[crawlNoteLines.length - 1];
  if (lastLine === line) {
    return;
  }

  crawlNoteLines.push(line);
  if (crawlNoteLines.length > 220) {
    crawlNoteLines = crawlNoteLines.slice(-220);
  }

  if (crawlNotes) {
    crawlNotes.textContent = crawlNoteLines.join("\n");
    crawlNotes.scrollTop = crawlNotes.scrollHeight;
  }

  if (errorBox) {
    errorBox.open = true;
  }
}

function resetCrawlNotes() {
  crawlNoteLines = [];
  latestStatusFingerprint = "";
  seenErrorNotes = new Set();
  seenInfoNotes = new Set();
  pushCrawlNote("Crawler initialized. Waiting for job start.");
}

function renderJob(job) {
  setPill(job.status);
  statDiscovered.textContent = job.discovered_pages;
  statScraped.textContent = job.scraped_pages;
  statAccepted.textContent = job.accepted_pages;
  statRejected.textContent = job.rejected_pages;
  crawlProgress.max = Math.max(job.max_pages, 1);
  crawlProgress.value = Math.min(job.scraped_pages, job.max_pages);
  const latestError = job.errors && job.errors.length ? job.errors[job.errors.length - 1] : "";
  const composedStatus = latestError
    ? `${job.status_message || job.status} Latest note: ${latestError}`
    : (job.status_message || job.status);
  setStatus(composedStatus, job.status === "failed");

  const statusFingerprint = `${job.status}|${job.status_message}|${job.scraped_pages}|${job.accepted_pages}|${job.rejected_pages}`;
  if (statusFingerprint !== latestStatusFingerprint) {
    latestStatusFingerprint = statusFingerprint;
    pushCrawlNote(
      `${job.status.toUpperCase()} -> discovered=${job.discovered_pages}, scraped=${job.scraped_pages}, accepted=${job.accepted_pages}, rejected=${job.rejected_pages}`
    );
    if (job.status_message) {
      pushCrawlNote(job.status_message);
    }
  }

  for (const message of job.errors || []) {
    if (seenErrorNotes.has(message)) {
      continue;
    }
    seenErrorNotes.add(message);
    pushCrawlNote(message, "WARN");
  }

  for (const message of job.messages || []) {
    if (seenInfoNotes.has(message)) {
      continue;
    }
    seenInfoNotes.add(message);
    pushCrawlNote(message);
  }

  const previewRecords = (job.records_preview || []).map((record) => record.data);
  datasetPreview.value = JSON.stringify(previewRecords, null, 2);
  downloadPreview.disabled = previewRecords.length === 0;
  renderPlatformProgress(job.platform_progress || []);

  const isComplete = job.status === "completed";
  setDownloadLink(downloadJson, isComplete ? job.download_json_url : null);
  setDownloadLink(downloadJsonl, isComplete ? job.download_jsonl_url : null);
  setDownloadLink(downloadCsv, isComplete ? job.download_csv_url : null);

  const canPause = job.status === "running";
  const canResume = job.status === "paused";
  const canCancel = job.status === "running" || job.status === "paused" || job.status === "queued";
  crawlPause.disabled = !canPause;
  crawlResume.disabled = !canResume;
  crawlCancel.disabled = !canCancel;

  if (job.status === "completed" || job.status === "failed" || job.status === "cancelled") {
    window.localStorage.removeItem(ACTIVE_CRAWL_KEY);
    activeCrawlJobId = null;
  }
}

async function pollJob(jobId) {
  try {
    const response = await fetch(`/api/crawl/${jobId}`);
    const job = await response.json();
    if (!response.ok) {
      throw new Error(job.detail || "Unable to read crawl status.");
    }

    renderJob(job);

    if (job.status === "completed" || job.status === "failed" || job.status === "cancelled") {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  } catch (error) {
    clearInterval(pollTimer);
    pollTimer = null;
    setStatus(error.message, true);
    pushCrawlNote(error.message, "ERROR");
  }
}

function readFileAsBase64(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => {
      const result = String(reader.result || "");
      resolve(result.split(",")[1] || "");
    };
    reader.onerror = () => reject(new Error("The selected file could not be read."));
    reader.readAsDataURL(file);
  });
}

function downloadBlob(filename, content, type) {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function setMergeStatus(message, isError = false) {
  mergeStatus.textContent = message;
  mergeStatus.classList.toggle("error", isError);
}

function setMergeProgress(value) {
  mergeProgress.value = value;
}

function setMergeDownloadState(enabled) {
  mergeDownload.hidden = !enabled;
  mergeDownload.disabled = !enabled;
}

function parseEditedPreview() {
  try {
    return JSON.parse(datasetPreview.value);
  } catch (error) {
    setStatus("Fix the JSON preview before downloading the edited preview.", true);
    throw error;
  }
}

function updateMergeControls() {
  const hasFiles = (mergeSourceFiles.files || []).length > 0;
  if (!hasFiles) {
    setMergeDownloadState(false);
    mergedDownloadState = null;
    setMergeProgress(0);
    setMergeStatus("Ready to merge JSON datasets.");
  } else {
    setMergeStatus(`${mergeSourceFiles.files.length} JSON file(s) selected.`);
  }
}

function parseWebsiteUrls() {
  const values = websiteUrlsInput.value
    .split(/[\r\n,;\t]+/)
    .map((value) => value.trim().replace(/^['"]+|['"]+$/g, ""))
    .filter(Boolean);

  const unique = [];
  const seen = new Set();
  for (const value of values) {
    const key = value.toLowerCase();
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    unique.push(value);
  }

  return unique;
}

function setActiveCrawlJob(jobId) {
  activeCrawlJobId = jobId || null;
  if (!jobId) {
    window.localStorage.removeItem(ACTIVE_CRAWL_KEY);
    return;
  }

  window.localStorage.setItem(ACTIVE_CRAWL_KEY, jobId);
}

async function controlCrawl(action) {
  if (!activeCrawlJobId) {
    pushCrawlNote("No active crawl job to control.", "WARN");
    return;
  }

  try {
    const response = await fetch(`/api/crawl/${activeCrawlJobId}/control`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ action }),
    });
    const job = await response.json();
    if (!response.ok) {
      throw new Error(job.detail || "Unable to control crawl.");
    }

    renderJob(job);
    pushCrawlNote(`Control action applied: ${action.toUpperCase()}.`);

    if (action === "resume") {
      restartJobPolling(activeCrawlJobId, true);
    }
  } catch (error) {
    pushCrawlNote(error.message, "ERROR");
    setStatus(error.message, true);
  }
}

mergeSourceFiles.addEventListener("change", () => {
  updateMergeControls();
  window.setTimeout(updateMergeControls, 0);
});

mergeSourceFiles.addEventListener("input", updateMergeControls);

crawlPause.addEventListener("click", () => {
  void controlCrawl("pause");
});

crawlResume.addEventListener("click", () => {
  void controlCrawl("resume");
});

crawlCancel.addEventListener("click", () => {
  void controlCrawl("cancel");
});

crawlReset.addEventListener("click", async () => {
  crawlReset.disabled = true;
  try {
    setStatus("Resetting platform state and freeing runtime memory...");
    let { response, payload } = await postResetRequest("/api/crawl/reset");

    if (!response.ok && response.status === 405) {
      ({ response, payload } = await postResetRequest("/api/crawl/reset/"));
    }

    if (!response.ok && response.status === 405) {
      if (activeCrawlJobId) {
        try {
          await fetch(`/api/crawl/${activeCrawlJobId}/control`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ action: "cancel" }),
          });
        } catch {
          // Ignore fallback cancellation errors and continue with local reset.
        }
      }

      restartJobPolling(null, false);
      resetCrawlUiState();
      resetCrawlNotes();
      setPill("idle");
      setStatus("Reset partially applied. Restart backend server and click Reset All again.", true);
      pushCrawlNote("Friend recited everything.");
      pushCrawlNote(
        "Backend still serves the old routes (405 Method Not Allowed). Restart server: ./.venv/bin/python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000",
        "WARN"
      );
      return;
    }

    if (!response.ok) {
      throw new Error(payload.detail || "Unable to reset platform state.");
    }

    restartJobPolling(null, false);
    resetCrawlUiState();
    resetCrawlNotes();
    setPill("idle");
    setStatus("Platform reset complete. Memory and crawl state cleared.");
    pushCrawlNote("Friend recited everything.");
    pushCrawlNote(
      `Reset summary: cleared_jobs=${payload.cleared_jobs ?? 0}, cancelled_tasks=${payload.cancelled_tasks ?? 0}.`
    );
  } catch (error) {
    setStatus(error.message, true);
    pushCrawlNote(error.message, "ERROR");
  } finally {
    crawlReset.disabled = false;
  }
});

async function runMerge() {
  const files = Array.from(mergeSourceFiles.files || []);
  if (!files.length) {
    setMergeStatus("Choose one or more JSON files to merge.", true);
    return;
  }

  setMergeDownloadState(false);
  mergedDownloadState = null;
  setMergeProgress(5);
  setMergeStatus("Preparing file(s) for deduplication...");

  try {
    const payloadFiles = [];
    for (let index = 0; index < files.length; index += 1) {
      const file = files[index];
      payloadFiles.push({
        filename: file.name,
        content_base64: await readFileAsBase64(file),
      });
      setMergeProgress(Math.min(15 + ((index + 1) / files.length) * 40, 55));
    }

    setMergeStatus("Deduplicating JSON records...");
    setMergeProgress(65);

    const response = await fetch("/api/merge-json", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ files: payloadFiles }),
    });

    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.detail || "JSON merge failed.");
    }

    setMergeProgress(90);
    datasetPreview.value = JSON.stringify(result.records || [], null, 2);
    downloadPreview.disabled = (result.records || []).length === 0;
    mergedDownloadState = {
      filename: "merged-medical-dataset.json",
      content: JSON.stringify(result.records || [], null, 2),
    };
    setMergeDownloadState((result.records || []).length > 0);
    setMergeProgress(100);
    const fileWord = result.source_file_count === 1 ? "file" : "files";
    setMergeStatus(
      `Produced ${result.merged_count} unique records from ${result.source_file_count} ${fileWord}. Removed ${result.duplicate_count} duplicates.`
    );
  } catch (error) {
    mergedDownloadState = null;
    setMergeDownloadState(false);
    setMergeProgress(0);
    setMergeStatus(error.message, true);
  }
}

mergeForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  await runMerge();
});

mergeDownload.addEventListener("click", () => {
  if (!mergedDownloadState) {
    return;
  }

  downloadBlob(
    mergedDownloadState.filename,
    mergedDownloadState.content,
    "application/json"
  );
});

crawlForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }

  resetDownloads();
  datasetPreview.value = "[]";
  setPill("queued");
  setStatus("Starting platform crawl...");
  resetCrawlNotes();

  const urls = parseWebsiteUrls();
  if (!urls.length) {
    setStatus("Enter at least one trusted website link.", true);
    pushCrawlNote("No website URLs were provided.", "ERROR");
    return;
  }

  pushCrawlNote(`Starting crawl with ${urls.length} platform(s).`);

  const payload = {
    url: urls[0],
    urls,
    max_pages: asNumber(maxPagesInput, 250),
    max_depth: asNumber(maxDepthInput, 2),
    concurrency: asNumber(concurrencyInput, 2),
    include_sitemap: includeSitemapInput.checked,
  };

  try {
    const response = await fetch("/api/crawl", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const job = await response.json();
    if (!response.ok) {
      throw new Error(job.detail || "Crawl could not start.");
    }

    renderJob(job);
    setActiveCrawlJob(job.job_id);
    restartJobPolling(job.job_id, true);
  } catch (error) {
    setPill("failed");
    setStatus(error.message, true);
    pushCrawlNote(error.message, "ERROR");
  }
});

downloadPreview.addEventListener("click", () => {
  const records = parseEditedPreview();
  downloadBlob("medical-crawl-preview.json", JSON.stringify(records, null, 2), "application/json");
});

fileForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  const file = fileInput.files[0];
  if (!file) {
    setStatus("Choose a PDF, TXT, or DOCX file.", true);
    return;
  }

  resetDownloads();
  setPill("running");
  setStatus("Extracting the selected file...");

  try {
    const response = await fetch("/api/extract", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        file: {
          filename: file.name,
          content_base64: await readFileAsBase64(file),
        },
      }),
    });

    const result = await response.json();
    if (!response.ok) {
      throw new Error(result.detail || "File extraction failed.");
    }

    const records = [result.data];
    datasetPreview.value = JSON.stringify(records, null, 2);
    downloadPreview.disabled = false;
    statDiscovered.textContent = "1";
    statScraped.textContent = "1";
    statAccepted.textContent = "1";
    statRejected.textContent = "0";
    crawlProgress.max = 1;
    crawlProgress.value = 1;
    setPill("completed");
    setStatus("File record is ready in the editable preview.");
  } catch (error) {
    setPill("failed");
    setStatus(error.message, true);
  }
});

updateMergeControls();
resetCrawlNotes();

if (metricsTimer) {
  clearInterval(metricsTimer);
}
metricsTimer = setInterval(() => {
  void pollMetrics();
}, 5000);
void pollMetrics();

const savedCrawlJobId = window.localStorage.getItem(ACTIVE_CRAWL_KEY);
if (savedCrawlJobId) {
  setActiveCrawlJob(savedCrawlJobId);
  setPill("queued");
  setStatus("Resuming the last crawl from saved progress...");
  pushCrawlNote(`Reconnecting to saved crawl job ${savedCrawlJobId}.`);
  restartJobPolling(savedCrawlJobId, true);
}
