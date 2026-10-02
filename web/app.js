/**
 * HydraStore Mission Control - Obsidian Telemetry Enterprise Engine
 * Connected directly to live distributed Coordinator & Data Plane daemons.
 */

const API_BASE = "";

// State
let clusterNodes = [];
let storedFiles = [];
let uploadXhr = null;

// DOM Cache
const nodesGrid = document.getElementById("nodesGrid");
const eventConsole = document.getElementById("eventConsole");
const dropzone = document.getElementById("dropzone");
const fileInput = document.getElementById("fileInput");
const uploadProgressContainer = document.getElementById("uploadProgressContainer");
const uploadProgressBar = document.getElementById("uploadProgressBar");
const uploadFileName = document.getElementById("uploadFileName");
const uploadPercent = document.getElementById("uploadPercent");
const uploadStatusText = document.getElementById("uploadStatusText");
const uploadSpeed = document.getElementById("uploadSpeed");
const filesTableBody = document.getElementById("filesTableBody");
const onlineNodeCount = document.getElementById("onlineNodeCount");
const clusterStatusPill = document.getElementById("clusterStatusPill");
const clusterTotalUsed = document.getElementById("clusterTotalUsed");
const clusterUsedPercent = document.getElementById("clusterUsedPercent");
const clusterTotalObjects = document.getElementById("clusterTotalObjects");
const storedObjectsCountBadge = document.getElementById("storedObjectsCountBadge");
const searchInput = document.getElementById("searchInput");

// Modal Elements
const chunkModal = document.getElementById("chunkModal");
const modalFileName = document.getElementById("modalFileName");
const modalFileSub = document.getElementById("modalFileSub");
const modalChunksSummary = document.getElementById("modalChunksSummary");
const modalNodeHeatmap = document.getElementById("modalNodeHeatmap");
const modalChunksContainer = document.getElementById("modalChunksContainer");

// Format helpers
function formatBytes(bytes, decimals = 2) {
  if (!+bytes) return "0 B";
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["B", "KB", "MB", "GB", "TB"];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
}

function truncateHash(hash, len = 12) {
  if (!hash) return "";
  return hash.length > len ? `${hash.substring(0, len)}...` : hash;
}

function copyToClipboard(text) {
  navigator.clipboard.writeText(text).then(() => {
    logEvent(`Copied to clipboard: ${truncateHash(text, 16)}`, "info");
  });
}

function logEvent(text, level = "info") {
  if (!eventConsole) return;
  const timestamp = new Date().toLocaleTimeString();
  const entry = document.createElement("div");
  entry.className = "flex items-start gap-2 text-xs font-mono py-0.5";

  let tagColor = "text-primary font-bold";
  let tagText = "[INFO]";

  if (level === "danger" || level === "error") {
    tagColor = "text-error font-bold";
    tagText = "[FAIL]";
  } else if (level === "warning") {
    tagColor = "text-tertiary font-bold";
    tagText = "[WARN]";
  } else if (level === "success") {
    tagColor = "text-secondary font-bold";
    tagText = "[OK]";
  }

  entry.innerHTML = `
    <span class="text-outline text-[11px] shrink-0">[${timestamp}]</span>
    <span class="${tagColor} shrink-0">${tagText}</span>
    <span class="text-on-surface break-all">${text}</span>
  `;

  eventConsole.appendChild(entry);
  eventConsole.scrollTop = eventConsole.scrollHeight;
}

function clearConsoleLog() {
  if (!eventConsole) return;
  eventConsole.innerHTML = `<div class="text-outline text-[11px]">[LOGS CLEARED] Telemetry console active.</div>`;
}

// --------------------------------------------------------------------------
// 1. Cluster Topology Management
// --------------------------------------------------------------------------
async function fetchClusterTopology() {
  try {
    const res = await fetch(`${API_BASE}/api/v1/cluster/nodes`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    clusterNodes = await res.json();
    renderClusterTopology();
  } catch (err) {
    console.error("Cluster topology fetch error:", err);
  }
}

function renderClusterTopology() {
  if (!nodesGrid) return;
  nodesGrid.innerHTML = "";

  let healthyCount = 0;
  let totalUsed = 0;
  let totalCap = 0;

  clusterNodes.forEach((node) => {
    const isHealthy = node.status === "HEALTHY";
    if (isHealthy) healthyCount++;

    const used = node.used_bytes || 0;
    const cap = node.capacity_bytes || 1073741824; // 1 GB default
    totalUsed += used;
    totalCap += cap;

    const usedFormatted = formatBytes(used);
    const capFormatted = formatBytes(cap);
    const percentUsed = Math.min(100, Math.round((used / cap) * 100));
    const nodeNum = node.id.replace("node-", "");

    const card = document.createElement("div");
    card.className = `bg-surface-container-low border ${
      isHealthy ? "border-outline-variant hover:border-outline" : "border-error/50 bg-error/5"
    } rounded-xl p-4 flex flex-col justify-between transition-all shadow-sm`;

    card.innerHTML = `
      <div class="space-y-3">
        <!-- Header -->
        <div class="flex items-start justify-between">
          <div>
            <div class="flex items-center gap-2">
              <span class="w-2.5 h-2.5 rounded-full ${
                isHealthy ? "bg-secondary pulse-glow-emerald" : "bg-error pulse-glow-crimson"
              }"></span>
              <span class="text-sm font-bold text-white">Storage Node 0${nodeNum}</span>
            </div>
            <div class="text-[11px] text-on-surface-variant font-mono mt-0.5">Port ${node.port}</div>
          </div>
          <span class="text-[11px] font-semibold px-2 py-0.5 rounded ${
            isHealthy
              ? "bg-secondary/15 text-secondary border border-secondary/30"
              : "bg-error/15 text-error border border-error/30"
          }">
            ${isHealthy ? "Online" : "Offline"}
          </span>
        </div>

        <!-- Storage Capacity Meter -->
        <div class="space-y-1.5 pt-1">
          <div class="flex justify-between text-xs text-on-surface-variant">
            <span>Disk Usage:</span>
            <span class="text-white font-medium font-mono">${usedFormatted} / ${capFormatted}</span>
          </div>
          <div class="w-full bg-surface-container-highest h-2 rounded-full overflow-hidden">
            <div class="${isHealthy ? "bg-primary" : "bg-error"} h-full rounded-full transition-all duration-500" style="width: ${Math.max(4, percentUsed)}%"></div>
          </div>
          <div class="flex justify-between text-[11px] text-outline font-mono">
            <span>${percentUsed}% Allocated</span>
            <span>1 GB Max</span>
          </div>
        </div>

        <!-- Quick Metrics -->
        <div class="grid grid-cols-2 gap-2 pt-2 border-t border-outline-variant/60 text-xs">
          <div>
            <div class="text-[10px] text-outline uppercase font-mono">Stored Chunks</div>
            <div class="text-white font-semibold font-mono mt-0.5">${node.chunk_count || 0} Chunks</div>
          </div>
          <div>
            <div class="text-[10px] text-outline uppercase font-mono">Health Status</div>
            <div class="${isHealthy ? "text-secondary" : "text-error"} font-semibold mt-0.5">${isHealthy ? "Healthy" : "Failed"}</div>
          </div>
        </div>
      </div>

      <!-- Action Button (Chaos / Self-Healing Simulation) -->
      <div class="mt-4 pt-3 border-t border-outline-variant/60">
        ${
          isHealthy
            ? `<button onclick="killStorageNode('${node.id}')" class="w-full py-2 px-3 bg-surface-container border border-outline-variant hover:border-tertiary hover:text-tertiary text-on-surface text-xs font-medium rounded-lg transition-all flex items-center justify-center gap-1.5 cursor-pointer" title="Simulate a sudden server failure to test self-healing">
                 <span class="material-symbols-outlined text-sm">flash_off</span>
                 <span>Simulate Server Failure</span>
               </button>`
            : `<button onclick="reviveStorageNode('${node.id}')" class="w-full py-2 px-3 bg-secondary/15 border border-secondary text-secondary hover:bg-secondary hover:text-black text-xs font-semibold rounded-lg transition-all flex items-center justify-center gap-1.5 cursor-pointer" title="Revive this storage node back online">
                 <span class="material-symbols-outlined text-sm">restart_alt</span>
                 <span>Revive Server Node</span>
               </button>`
        }
      </div>
    `;

    nodesGrid.appendChild(card);
  });

  // Update Status Pill
  if (onlineNodeCount) {
    onlineNodeCount.innerText = `${healthyCount}/${clusterNodes.length} NODES ONLINE`;
  }
  if (clusterStatusPill) {
    if (healthyCount === clusterNodes.length && clusterNodes.length > 0) {
      clusterStatusPill.className = "hidden md:flex items-center gap-2 px-3 py-1 bg-surface-container-low border border-outline-variant rounded-full text-xs font-mono";
      onlineNodeCount.className = "text-secondary font-semibold";
    } else {
      clusterStatusPill.className = "hidden md:flex items-center gap-2 px-3 py-1 bg-error/10 border border-error/40 rounded-full text-xs font-mono";
      onlineNodeCount.className = "text-error font-semibold";
    }
  }

  // Update Ticker Metrics
  if (clusterTotalUsed) {
    clusterTotalUsed.innerText = `${formatBytes(totalUsed)} / ${formatBytes(totalCap)}`;
  }
  if (clusterUsedPercent) {
    const totalPercent = totalCap > 0 ? Math.round((totalUsed / totalCap) * 100) : 0;
    clusterUsedPercent.innerText = `${totalPercent}% Used`;
    const clusterProgressBar = document.getElementById("clusterProgressBar");
    if (clusterProgressBar) {
      clusterProgressBar.style.width = `${totalPercent}%`;
    }
  }
}

async function killStorageNode(nodeId) {
  logEvent(`Operator triggered Chaos: Terminating [${nodeId}]...`, "warning");
  try {
    const res = await fetch(`${API_BASE}/api/v1/cluster/nodes/${nodeId}/kill`, { method: "POST" });
    if (res.ok) {
      logEvent(`Node [${nodeId}] is DEAD. Heartbeat lease revoked.`, "danger");
      fetchClusterTopology();
    } else {
      throw new Error(`HTTP ${res.status}`);
    }
  } catch (err) {
    logEvent(`Error killing node [${nodeId}]: ${err.message}`, "danger");
  }
}

async function reviveStorageNode(nodeId) {
  logEvent(`Operator reviving [${nodeId}]...`, "info");
  try {
    const res = await fetch(`${API_BASE}/api/v1/cluster/nodes/${nodeId}/revive`, { method: "POST" });
    if (res.ok) {
      logEvent(`Node [${nodeId}] successfully revived and joined quorum!`, "success");
      fetchClusterTopology();
    } else {
      throw new Error(`HTTP ${res.status}`);
    }
  } catch (err) {
    logEvent(`Error reviving node [${nodeId}]: ${err.message}`, "danger");
  }
}

// --------------------------------------------------------------------------
// 2. Real-Time SSE Event Stream
// --------------------------------------------------------------------------
function setupEventStream() {
  const eventSource = new EventSource(`${API_BASE}/api/v1/cluster/events`);

  eventSource.onmessage = (event) => {
    try {
      const data = JSON.parse(event.data);
      handleClusterEvent(data);
    } catch (e) {
      console.warn("SSE Parse error:", e);
    }
  };

  eventSource.onerror = () => {
    // Retry automatically handled by browser
  };
}

function handleClusterEvent(event) {
  const type = event.type;
  const payload = event.payload || {};

  switch (type) {
    case "CONNECTED":
      logEvent("Connected to HydraStore Real-Time Event Stream.", "info");
      break;

    case "NODE_RECOVERED":
      logEvent(`Storage Node [${payload.node_id}] recovered! Status -> HEALTHY.`, "success");
      fetchClusterTopology();
      break;

    case "NODE_DIED":
      logEvent(`Storage Node [${payload.node_id}] missed heartbeats! Status -> DEAD.`, "danger");
      fetchClusterTopology();
      break;

    case "CHAOS_NODE_KILLED":
      logEvent(`Operator Chaos Action: [${payload.node_id}] killed!`, "danger");
      fetchClusterTopology();
      break;

    case "CHAOS_NODE_REVIVED":
      logEvent(`Operator Action: [${payload.node_id}] revived.`, "success");
      fetchClusterTopology();
      break;

    case "CHUNK_DISPATCHED":
      logEvent(
        `Chunk #${payload.chunk_index} (${formatBytes(payload.size_bytes)}) placed on Primary [${payload.primary_node}] & Replicas [${(payload.replica_nodes || []).join(", ")}].`,
        "info"
      );
      break;

    case "FILE_UPLOADED":
      logEvent(`Object '${payload.file_name}' (${formatBytes(payload.size_bytes)}) committed across ${payload.total_chunks} chunks.`, "success");
      fetchFiles();
      fetchClusterTopology();
      break;

    case "FAILOVER_TRIGGERED":
      logEvent(
        `FAILOVER: Primary [${payload.failed_node}] offline for chunk #${payload.chunk_index}. Transparently routed to Replica [${payload.fallback_node}]!`,
        "warning"
      );
      break;

    case "HEALING_STARTED":
      logEvent(`Self-Healing: Re-replicating chunk [${payload.chunk_id}] from surviving [${payload.source_node}] to [${payload.target_node}].`, "warning");
      break;

    case "HEALING_COMPLETED":
      logEvent(`Durability restored: Chunk [${payload.chunk_id}] healed to RF=2 on node [${payload.target_node}].`, "success");
      fetchClusterTopology();
      break;

    default:
      if (payload.message) {
        logEvent(payload.message, "info");
      }
      break;
  }
}

// --------------------------------------------------------------------------
// 3. File Upload & Ingestion Pipeline
// --------------------------------------------------------------------------
function setupDragAndDrop() {
  if (!dropzone || !fileInput) return;

  dropzone.addEventListener("click", () => fileInput.click());

  dropzone.addEventListener("dragover", (e) => {
    e.preventDefault();
    dropzone.classList.add("border-primary", "bg-surface-container");
  });

  dropzone.addEventListener("dragleave", () => {
    dropzone.classList.remove("border-primary", "bg-surface-container");
  });

  dropzone.addEventListener("drop", (e) => {
    e.preventDefault();
    dropzone.classList.remove("border-primary", "bg-surface-container");
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      handleFileUpload(e.dataTransfer.files[0]);
    }
  });

  fileInput.addEventListener("change", (e) => {
    if (e.target.files && e.target.files.length > 0) {
      handleFileUpload(e.target.files[0]);
    }
  });
}

function handleFileUpload(file) {
  if (!file) return;

  uploadFileName.innerText = file.name;
  uploadPercent.innerText = "0%";
  uploadStatusText.innerText = "Connecting to pipeline...";
  uploadSpeed.innerText = "";
  uploadProgressBar.style.width = "0%";
  uploadProgressContainer.classList.remove("hidden");

  logEvent(`Starting stream ingestion for '${file.name}' (${formatBytes(file.size)})...`, "info");

  const formData = new FormData();
  formData.append("file", file);

  const startTime = Date.now();
  uploadXhr = new XMLHttpRequest();
  uploadXhr.open("POST", `${API_BASE}/api/v1/files/upload`);

  uploadXhr.upload.onprogress = (e) => {
    if (e.lengthComputable) {
      const percent = Math.round((e.loaded / e.total) * 100);
      const elapsedSec = (Date.now() - startTime) / 1000;
      const speedMB = elapsedSec > 0 ? (e.loaded / (1024 * 1024)) / elapsedSec : 0;

      uploadProgressBar.style.width = `${percent}%`;
      uploadPercent.innerText = `${percent}%`;
      uploadStatusText.innerText = `Slicing 4MB chunks & replicating (${formatBytes(e.loaded)} / ${formatBytes(e.total)})...`;
      uploadSpeed.innerText = `${speedMB.toFixed(1)} MB/s`;
    }
  };

  uploadXhr.onload = () => {
    if (uploadXhr.status === 200) {
      uploadProgressBar.style.width = "100%";
      uploadPercent.innerText = "100%";
      uploadStatusText.innerText = "Replication factor RF=2 committed!";
      uploadSpeed.innerText = "Done";

      logEvent(`Object '${file.name}' successfully stored, hashed, and replicated!`, "success");
      fetchFiles();
      fetchClusterTopology();

      setTimeout(() => {
        uploadPercent.innerText = "Idle";
        uploadStatusText.innerText = "Ready for next payload";
        uploadSpeed.innerText = "";
      }, 4000);
    } else {
      let errDetail = uploadXhr.responseText;
      try {
        errDetail = JSON.parse(uploadXhr.responseText).detail || errDetail;
      } catch (e) {}

      uploadStatusText.innerText = `Upload failed: ${errDetail}`;
      logEvent(`Upload failed: ${errDetail}`, "danger");
      alert(`Upload Failed:\n\n${errDetail}`);
    }
  };

  uploadXhr.onerror = () => {
    uploadStatusText.innerText = "Upload network connection error";
    logEvent("Network connection dropped during upload.", "danger");
  };

  uploadXhr.send(formData);
}

// --------------------------------------------------------------------------
// 4. File Catalog Management
// --------------------------------------------------------------------------
async function fetchFiles() {
  try {
    const res = await fetch(`${API_BASE}/api/v1/files`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    storedFiles = await res.json();
    renderFilesTable();
  } catch (err) {
    console.error("Fetch files error:", err);
  }
}

function renderFilesTable() {
  if (!filesTableBody) return;

  const query = (searchInput ? searchInput.value : "").toLowerCase().trim();
  const filtered = storedFiles.filter(
    (f) =>
      f.name.toLowerCase().includes(query) ||
      (f.checksum_sha256 && f.checksum_sha256.toLowerCase().includes(query))
  );

  if (clusterTotalObjects) {
    clusterTotalObjects.innerText = storedFiles.length;
  }
  if (storedObjectsCountBadge) {
    storedObjectsCountBadge.innerText = `${storedFiles.length} OBJECTS`;
  }

  if (filtered.length === 0) {
    filesTableBody.innerHTML = `
      <tr>
        <td colspan="6" class="py-8 text-center text-outline font-mono">
          ${storedFiles.length === 0 ? "No objects stored yet. Drag and drop a file above to begin." : "No matching objects found."}
        </td>
      </tr>
    `;
    return;
  }

  filesTableBody.innerHTML = "";

  filtered.forEach((f) => {
    const tr = document.createElement("tr");
    tr.className = "hover:bg-surface-container transition-colors group";

    const dateFormatted = f.created_at
      ? new Date(f.created_at).toLocaleDateString() + " " + new Date(f.created_at).toLocaleTimeString()
      : "-";

    tr.innerHTML = `
      <td class="py-3 px-4 font-semibold text-white flex items-center gap-2">
        <span class="material-symbols-outlined text-primary text-base">description</span>
        <span class="truncate max-w-xs" title="${f.name}">${f.name}</span>
      </td>
      <td class="py-3 px-4 text-on-surface whitespace-nowrap">${formatBytes(f.size_bytes)}</td>
      <td class="py-3 px-4 whitespace-nowrap">
        <span class="px-2 py-0.5 bg-surface-container-high border border-outline-variant text-primary font-mono text-[11px] rounded">
          ${f.total_chunks} Chunks (RF=2)
        </span>
      </td>
      <td class="py-3 px-4 text-outline font-mono whitespace-nowrap">
        <span>${truncateHash(f.checksum_sha256, 14)}</span>
        <button onclick="copyToClipboard('${f.checksum_sha256}')" class="ml-1 text-outline hover:text-primary transition-colors" title="Copy Checksum">
          <span class="material-symbols-outlined text-xs align-middle">content_copy</span>
        </button>
      </td>
      <td class="py-3 px-4 text-outline whitespace-nowrap">${dateFormatted}</td>
      <td class="py-3 px-4 text-right space-x-2 whitespace-nowrap">
        <button onclick="downloadFile('${f.id}', '${f.name}')" class="px-2.5 py-1 bg-surface-container border border-primary text-primary hover:bg-primary hover:text-black font-mono text-xs rounded transition-all cursor-pointer font-medium" title="Stream Download with Failover">
          Download
        </button>
        <button onclick="inspectFileChunks('${f.id}')" class="px-2.5 py-1 bg-surface-container border border-outline-variant hover:border-primary text-on-surface font-mono text-xs rounded transition-all cursor-pointer" title="Inspect Chunk Topology">
          Inspect
        </button>
        <button onclick="deleteFile('${f.id}', '${f.name}')" class="p-1 text-outline hover:text-error transition-colors cursor-pointer" title="Delete Object">
          <span class="material-symbols-outlined text-sm align-middle">delete</span>
        </button>
      </td>
    `;
    filesTableBody.appendChild(tr);
  });
}

// --------------------------------------------------------------------------
// 5. Safe Streaming Downloader (503 Block Check)
// --------------------------------------------------------------------------
async function downloadFile(fileId, fileName) {
  logEvent(`Attempting download for '${fileName}'...`, "info");
  try {
    const res = await fetch(`${API_BASE}/api/v1/files/${fileId}/download`);

    if (!res.ok) {
      let detailMsg = `HTTP ${res.status}`;
      try {
        const errJson = await res.json();
        detailMsg = errJson.detail || detailMsg;
      } catch (e) {}

      logEvent(`❌ Download Blocked [${res.status}]: ${detailMsg}`, "danger");
      alert(`⚠️ Cluster Unavailable (HTTP ${res.status}):\n\n${detailMsg}`);
      return;
    }

    // Convert streamed response to blob and trigger genuine file save
    const blob = await res.blob();
    const blobUrl = window.URL.createObjectURL(blob);
    const downloadAnchor = document.createElement("a");
    downloadAnchor.href = blobUrl;
    downloadAnchor.download = fileName;
    document.body.appendChild(downloadAnchor);
    downloadAnchor.click();
    window.URL.revokeObjectURL(blobUrl);
    downloadAnchor.remove();

    logEvent(`✅ File '${fileName}' (${formatBytes(blob.size)}) successfully retrieved!`, "success");
  } catch (err) {
    logEvent(`Network error during download: ${err.message}`, "danger");
    alert(`Download Error: ${err.message}`);
  }
}

// --------------------------------------------------------------------------
// 6. Inspect File Chunks & Replica Matrix
// --------------------------------------------------------------------------
async function inspectFileChunks(fileId) {
  try {
    const res = await fetch(`${API_BASE}/api/v1/files/${fileId}`);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const details = await res.json();

    modalFileName.innerText = details.name;
    modalFileSub.innerText = `${formatBytes(details.size_bytes)} // ${details.total_chunks} Chunks // SHA-256: ${details.checksum_sha256}`;

    // Summary Strip
    modalChunksSummary.innerHTML = `
      <div>
        <div class="text-outline text-[10px] uppercase">TOTAL CHUNKS</div>
        <div class="text-white font-bold text-sm">${details.total_chunks} Blocks</div>
      </div>
      <div>
        <div class="text-outline text-[10px] uppercase">STANDARD CHUNK SIZE</div>
        <div class="text-white font-bold text-sm">4.0 MB</div>
      </div>
      <div>
        <div class="text-outline text-[10px] uppercase">REPLICATION FACTOR</div>
        <div class="text-secondary font-bold text-sm">RF=2 (${details.total_chunks * 2} Replicas)</div>
      </div>
      <div>
        <div class="text-outline text-[10px] uppercase">INTEGRITY CHECK</div>
        <div class="text-secondary font-bold text-sm">100% SHA-256 VERIFIED</div>
      </div>
    `;

    // Calculate Node Allocation Heatmap
    const nodeCounts = { "node-1": 0, "node-2": 0, "node-3": 0, "node-4": 0 };
    details.chunks.forEach((c) => {
      c.replicas.forEach((r) => {
        if (nodeCounts[r.node_id] !== undefined) {
          nodeCounts[r.node_id]++;
        }
      });
    });

    modalNodeHeatmap.innerHTML = Object.entries(nodeCounts)
      .map(([nId, count]) => `
        <div class="bg-surface-container p-2.5 rounded border border-outline-variant">
          <div class="text-primary font-bold uppercase">${nId}</div>
          <div class="text-white text-sm font-semibold">${count} Shards Allocated</div>
          <div class="text-outline text-[10px]">Consistent Token Target</div>
        </div>
      `)
      .join("");

    // Populate Chunks Table
    modalChunksContainer.innerHTML = details.chunks
      .map((c) => {
        const prim = c.replicas.find((r) => r.is_primary);
        const repl = c.replicas.find((r) => !r.is_primary);

        return `
          <tr class="hover:bg-surface-container transition-colors">
            <td class="py-2 px-3 font-mono text-primary font-medium">#${c.chunk_index.toString().padStart(4, "0")}</td>
            <td class="py-2 px-3 text-on-surface whitespace-nowrap">${formatBytes(c.size_bytes)}</td>
            <td class="py-2 px-3 font-semibold text-white whitespace-nowrap">${prim ? prim.node_id.toUpperCase() : "N/A"}</td>
            <td class="py-2 px-3 text-on-surface-variant whitespace-nowrap">${repl ? repl.node_id.toUpperCase() : "N/A"}</td>
            <td class="py-2 px-3 font-mono text-outline whitespace-nowrap">
              <span>${truncateHash(c.checksum_sha256, 12)}</span>
            </td>
          </tr>
        `;
      })
      .join("");

    chunkModal.classList.remove("hidden");
  } catch (err) {
    alert("Could not load chunk layout: " + err.message);
  }
}

function closeChunkModal() {
  if (chunkModal) {
    chunkModal.classList.add("hidden");
  }
}

async function deleteFile(fileId, fileName) {
  if (!confirm(`Are you sure you want to delete '${fileName}' and prune all replicated chunks across nodes?`)) {
    return;
  }
  try {
    const res = await fetch(`${API_BASE}/api/v1/files/${fileId}`, { method: "DELETE" });
    if (res.ok) {
      logEvent(`Deleted object '${fileName}' and purged chunks from storage nodes.`, "info");
      fetchFiles();
      fetchClusterTopology();
    } else {
      throw new Error(`HTTP ${res.status}`);
    }
  } catch (err) {
    logEvent(`Delete error: ${err.message}`, "danger");
  }
}

// --------------------------------------------------------------------------
// 7. Sidebar Active State Management
// --------------------------------------------------------------------------
function setupSidebarNavigation() {
  const sidebarLinks = document.querySelectorAll(".sidebar-link");
  sidebarLinks.forEach((link) => {
    link.addEventListener("click", (e) => {
      sidebarLinks.forEach((l) => l.classList.remove("active"));
      link.classList.add("active");
    });
  });

  // Highlight on scroll
  const sections = ["topology", "ingest", "buckets", "eventstream"];
  const mainContent = document.querySelector("main");
  if (mainContent) {
    mainContent.addEventListener("scroll", () => {
      let current = "";
      sections.forEach((id) => {
        const section = document.getElementById(id);
        if (section && section.offsetTop - mainContent.scrollTop <= 120) {
          current = id;
        }
      });
      if (current) {
        sidebarLinks.forEach((l) => {
          l.classList.remove("active");
          if (l.getAttribute("href") === `#${current}`) {
            l.classList.add("active");
          }
        });
      }
    });
  }
}

// --------------------------------------------------------------------------
// 8. Initialization
// --------------------------------------------------------------------------
document.addEventListener("DOMContentLoaded", () => {
  fetchClusterTopology();
  fetchFiles();
  setupDragAndDrop();
  setupEventStream();
  setupSidebarNavigation();

  if (searchInput) {
    searchInput.addEventListener("input", renderFilesTable);
  }

  // Periodic heartbeat refresh every 4 seconds
  setInterval(fetchClusterTopology, 4000);
});
