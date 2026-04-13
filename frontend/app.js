const crawlForm = document.querySelector("#crawl-form");
const websiteUrlInput = document.querySelector("#website-url");
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
const crawlProgress = document.querySelector("#crawl-progress");
const datasetPreview = document.querySelector("#dataset-preview");
const downloadJson = document.querySelector("#download-json");
const downloadJsonl = document.querySelector("#download-jsonl");
const downloadCsv = document.querySelector("#download-csv");
const downloadPreview = document.querySelector("#download-preview");
const errorList = document.querySelector("#error-list");
const fileForm = document.querySelector("#file-form");
const fileInput = document.querySelector("#source-file");

let pollTimer = null;

function setStatus(message, isError = false) {
  crawlStatus.textContent = message;
  crawlStatus.classList.toggle("error", isError);
}

function setPill(status) {
  jobStatusPill.textContent = status;
  jobStatusPill.className = `status-pill ${status}`;
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

function renderErrors(errors = []) {
  errorList.replaceChildren();
  for (const message of errors.slice(-8)) {
    const item = document.createElement("li");
    item.textContent = message;
    errorList.appendChild(item);
  }
}

function renderJob(job) {
  setPill(job.status);
  statDiscovered.textContent = job.discovered_pages;
  statScraped.textContent = job.scraped_pages;
  statAccepted.textContent = job.accepted_pages;
  statRejected.textContent = job.rejected_pages;
  crawlProgress.max = Math.max(job.max_pages, 1);
  crawlProgress.value = Math.min(job.scraped_pages, job.max_pages);
  setStatus(job.status_message || job.status);
  renderErrors(job.errors || []);

  const previewRecords = (job.records_preview || []).map((record) => record.data);
  datasetPreview.value = JSON.stringify(previewRecords, null, 2);
  downloadPreview.disabled = previewRecords.length === 0;

  const isComplete = job.status === "completed";
  setDownloadLink(downloadJson, isComplete ? job.download_json_url : null);
  setDownloadLink(downloadJsonl, isComplete ? job.download_jsonl_url : null);
  setDownloadLink(downloadCsv, isComplete ? job.download_csv_url : null);
}

async function pollJob(jobId) {
  try {
    const response = await fetch(`/api/crawl/${jobId}`);
    const job = await response.json();
    if (!response.ok) {
      throw new Error(job.detail || "Unable to read crawl status.");
    }

    renderJob(job);

    if (job.status === "completed" || job.status === "failed") {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  } catch (error) {
    clearInterval(pollTimer);
    pollTimer = null;
    setStatus(error.message, true);
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

function parseEditedPreview() {
  try {
    return JSON.parse(datasetPreview.value);
  } catch (error) {
    setStatus("Fix the JSON preview before downloading the edited preview.", true);
    throw error;
  }
}

crawlForm.addEventListener("submit", async (event) => {
  event.preventDefault();

  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }

  resetDownloads();
  datasetPreview.value = "[]";
  setPill("queued");
  setStatus("Starting website crawl...");

  const payload = {
    url: websiteUrlInput.value.trim(),
    max_pages: asNumber(maxPagesInput, 1000),
    max_depth: asNumber(maxDepthInput, 3),
    concurrency: asNumber(concurrencyInput, 4),
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
    pollTimer = setInterval(() => pollJob(job.job_id), 1500);
    await pollJob(job.job_id);
  } catch (error) {
    setPill("failed");
    setStatus(error.message, true);
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
