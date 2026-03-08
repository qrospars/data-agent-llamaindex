const el = {
  chatForm: document.getElementById("chatForm"),
  chatInput: document.getElementById("chatInput"),
  chatLog: document.getElementById("chatLog"),
  sendBtn: document.getElementById("sendBtn"),
  chips: Array.from(document.querySelectorAll(".chip")),
  status: document.getElementById("statusPill"),
  dbUrl: document.getElementById("dbUrl"),
  semanticPath: document.getElementById("semanticPath"),
  llmProvider: document.getElementById("llmProvider"),
  llmModel: document.getElementById("llmModel"),
  sessionId: document.getElementById("sessionId"),
  modeValue: document.getElementById("modeValue"),
  rowCountValue: document.getElementById("rowCountValue"),
  chartTypeValue: document.getElementById("chartTypeValue"),
  notesPath: document.getElementById("notesPath"),
  sqlText: document.getElementById("sqlText"),
  resultTable: document.getElementById("resultTable"),
  chartEmpty: document.getElementById("chartEmpty"),
  chartCanvas: document.getElementById("resultChart"),
};

let chart = null;
let isSubmitting = false;

function setStatus(text, tone = "ready") {
  el.status.textContent = text;
  const palette =
    tone === "error"
      ? {
          border: "rgba(248, 113, 113, 0.5)",
          background: "rgba(248, 113, 113, 0.18)",
          color: "#ffd2d2",
        }
      : tone === "busy"
        ? {
            border: "rgba(14, 165, 233, 0.45)",
            background: "rgba(14, 165, 233, 0.14)",
            color: "#d2f0ff",
          }
        : {
            border: "rgba(45, 212, 191, 0.4)",
            background: "rgba(45, 212, 191, 0.12)",
            color: "#bffef3",
          };

  el.status.style.borderColor = palette.border;
  el.status.style.background = palette.background;
  el.status.style.color = palette.color;
}

function addMessage({ role, text, mode = "", sql = "", pending = false, reflectionSteps = [] }) {
  const block = document.createElement("div");
  block.className = `msg ${role === "user" ? "user" : "agent"}`;
  if (pending) {
    block.classList.add("pending");
  }

  const head = document.createElement("div");
  head.className = "msg-head";
  const roleSpan = document.createElement("span");
  roleSpan.textContent = role === "user" ? "You" : "Agent";
  head.appendChild(roleSpan);
  if (mode) {
    const modeSpan = document.createElement("span");
    modeSpan.className = "mode-badge";
    modeSpan.textContent = mode;
    head.appendChild(modeSpan);
  }
  block.appendChild(head);

  const paragraph = document.createElement("p");
  paragraph.textContent = text;
  block.appendChild(paragraph);

  if (sql) {
    const sqlPre = document.createElement("pre");
    sqlPre.className = "sql-inline";
    sqlPre.textContent = sql;
    block.appendChild(sqlPre);
  }

  const stepEls = [];
  if (reflectionSteps.length) {
    const stepsTitle = document.createElement("div");
    stepsTitle.className = "reflection-title";
    stepsTitle.textContent = "Progress";
    block.appendChild(stepsTitle);

    const list = document.createElement("ul");
    list.className = "reflection-steps";
    reflectionSteps.forEach((stepText, idx) => {
      const item = document.createElement("li");
      item.className = "reflection-step";
      if (idx === 0) {
        item.classList.add("is-active");
      }
      item.textContent = stepText;
      list.appendChild(item);
      stepEls.push(item);
    });
    block.appendChild(list);
  }

  el.chatLog.appendChild(block);
  el.chatLog.scrollTop = el.chatLog.scrollHeight;
  return { block, paragraph, stepEls };
}

function setInFlight(inFlight) {
  isSubmitting = inFlight;
  el.sendBtn.disabled = inFlight;
  el.chatInput.disabled = inFlight;
  el.chips.forEach((chip) => {
    chip.disabled = inFlight;
  });
}

function startThinkingFeedback() {
  const thinking = addMessage({
    role: "agent",
    mode: "thinking",
    text: "Thinking about your question",
    pending: true,
  });

  const stepsTitle = document.createElement("div");
  stepsTitle.className = "reflection-title";
  stepsTitle.textContent = "Progress";
  thinking.block.appendChild(stepsTitle);

  const stepsList = document.createElement("ul");
  stepsList.className = "reflection-steps";
  thinking.block.appendChild(stepsList);

  const stageToStep = new Map();
  const baseText = "Thinking about your question";
  let dotCount = 0;

  const dotTimer = window.setInterval(() => {
    dotCount = (dotCount + 1) % 4;
    thinking.paragraph.textContent = `${baseText}${".".repeat(dotCount)}`;
  }, 360);

  return {
    progress(stage, message) {
      const stepText = String(message || stage || "").trim();
      if (!stepText) {
        return;
      }
      const key = String(stage || stepText);
      let stepEl = stageToStep.get(key);
      if (!stepEl) {
        stepEl = document.createElement("li");
        stepEl.className = "reflection-step";
        stepEl.textContent = stepText;
        stepsList.appendChild(stepEl);
        stageToStep.set(key, stepEl);
      } else {
        stepEl.textContent = stepText;
      }

      stageToStep.forEach((otherEl) => {
        if (otherEl === stepEl) {
          otherEl.classList.add("is-active");
          otherEl.classList.remove("is-done");
        } else {
          otherEl.classList.remove("is-active");
          otherEl.classList.add("is-done");
        }
      });
      el.chatLog.scrollTop = el.chatLog.scrollHeight;
    },
    stop(remove = true) {
      window.clearInterval(dotTimer);
      stageToStep.forEach((stepEl) => {
        stepEl.classList.remove("is-active");
        stepEl.classList.add("is-done");
      });
      if (remove && thinking.block.isConnected) {
        thinking.block.remove();
      }
    },
  };
}

async function readResponsePayload(response) {
  const raw = await response.text();
  if (!raw) {
    return {};
  }
  try {
    return JSON.parse(raw);
  } catch {
    return { detail: raw };
  }
}

function parseSseBlock(rawBlock) {
  const lines = rawBlock.split("\n");
  let event = "message";
  const dataLines = [];

  lines.forEach((line) => {
    if (line.startsWith("event:")) {
      event = line.slice("event:".length).trim();
      return;
    }
    if (line.startsWith("data:")) {
      dataLines.push(line.slice("data:".length).trimStart());
    }
  });

  const rawData = dataLines.join("\n");
  if (!rawData) {
    return { event, data: {} };
  }
  try {
    return { event, data: JSON.parse(rawData) };
  } catch {
    return { event, data: { message: rawData } };
  }
}

async function streamChat(payload, onProgress) {
  const response = await fetch("/chat/stream", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  if (!response.ok) {
    const data = await readResponsePayload(response);
    const detail =
      typeof data.detail === "string"
        ? data.detail
        : data.detail
          ? JSON.stringify(data.detail)
          : `Request failed (${response.status})`;
    throw new Error(detail);
  }

  if (!response.body) {
    throw new Error("Streaming is not available in this browser.");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let finalPayload = null;
  let streamError = "";
  let sawDoneEvent = false;

  while (!sawDoneEvent) {
    const { done, value } = await reader.read();
    if (done) {
      break;
    }
    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");

    let boundary = buffer.indexOf("\n\n");
    while (boundary !== -1) {
      const block = buffer.slice(0, boundary).trim();
      buffer = buffer.slice(boundary + 2);
      if (block) {
        const parsed = parseSseBlock(block);
        if (parsed.event === "progress") {
          onProgress(parsed.data);
        } else if (parsed.event === "final") {
          finalPayload = parsed.data;
        } else if (parsed.event === "error") {
          streamError =
            typeof parsed.data.message === "string"
              ? parsed.data.message
              : "Request failed while streaming.";
        } else if (parsed.event === "done") {
          sawDoneEvent = true;
          await reader.cancel();
          break;
        }
      }
      boundary = buffer.indexOf("\n\n");
    }
  }

  if (streamError) {
    throw new Error(streamError);
  }
  if (!finalPayload) {
    throw new Error("No final response received from stream.");
  }
  return finalPayload;
}

function renderTable(columns, rows) {
  if (!columns || columns.length === 0 || !rows || rows.length === 0) {
    el.resultTable.innerHTML = "<div class='empty-state'>No row preview available.</div>";
    return;
  }

  const thead = columns.map((c) => `<th>${escapeHtml(String(c))}</th>`).join("");
  const body = rows
    .slice(0, 100)
    .map((row) => {
      const tds = row.map((cell) => `<td>${escapeHtml(String(cell ?? ""))}</td>`).join("");
      return `<tr>${tds}</tr>`;
    })
    .join("");

  el.resultTable.innerHTML = `<table><thead><tr>${thead}</tr></thead><tbody>${body}</tbody></table>`;
}

function renderChart(payload) {
  const columns = payload.columns || [];
  const rows = payload.rows || [];
  const chartSuggestion = payload.chart_suggestion || "";

  if (chart) {
    chart.destroy();
    chart = null;
  }

  if (!rows.length || columns.length < 2) {
    el.chartEmpty.style.display = "flex";
    el.chartTypeValue.textContent = "-";
    return;
  }

  const type = chartSuggestion === "line" ? "line" : chartSuggestion === "bar" ? "bar" : "";
  if (!type) {
    el.chartEmpty.style.display = "flex";
    el.chartTypeValue.textContent = chartSuggestion || "table";
    return;
  }

  const mapping = pickChartFields(columns, rows, type);
  if (!mapping) {
    el.chartEmpty.style.display = "flex";
    el.chartTypeValue.textContent = type;
    return;
  }

  const labels = rows.map((r) => String(r[mapping.labelIdx] ?? ""));
  const values = rows.map((r) => Number(r[mapping.valueIdx]));
  if (!values.every((v) => Number.isFinite(v))) {
    el.chartEmpty.style.display = "flex";
    el.chartTypeValue.textContent = type;
    return;
  }

  el.chartEmpty.style.display = "none";
  el.chartTypeValue.textContent = type;
  chart = new Chart(el.chartCanvas, {
    type,
    data: {
      labels,
      datasets: [
        {
          label: columns[mapping.valueIdx],
          data: values,
          borderColor: "#2dd4bf",
          backgroundColor: type === "bar" ? "rgba(45, 212, 191, 0.45)" : "rgba(45, 212, 191, 0.12)",
          borderWidth: 2,
          tension: 0.25,
          pointRadius: type === "line" ? 2.5 : 0,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          labels: { color: "#dbe8f6", font: { family: "Space Grotesk" } },
        },
      },
      scales: {
        x: {
          ticks: { color: "#8aa4bf", maxRotation: 30, minRotation: 0, font: { family: "IBM Plex Mono" } },
          grid: { color: "rgba(138, 164, 191, 0.1)" },
        },
        y: {
          ticks: { color: "#8aa4bf", font: { family: "IBM Plex Mono" } },
          grid: { color: "rgba(138, 164, 191, 0.12)" },
        },
      },
    },
  });
}

function pickChartFields(columns, rows, type) {
  const timeIdx = columns.findIndex((c) => /date|time/i.test(String(c)));
  const numericIndices = columns
    .map((_, idx) => idx)
    .filter((idx) => rows.some((r) => Number.isFinite(Number(r[idx]))));
  if (!numericIndices.length) {
    return null;
  }

  if (type === "line") {
    const labelIdx = timeIdx >= 0 ? timeIdx : 0;
    const valueIdx = numericIndices.find((idx) => idx !== labelIdx) ?? numericIndices[0];
    return { labelIdx, valueIdx };
  }

  const labelIdx = 0;
  const valueIdx = numericIndices.find((idx) => idx !== labelIdx) ?? numericIndices[0];
  return { labelIdx, valueIdx };
}

function escapeHtml(text) {
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function submitMessage(message) {
  const thinking = startThinkingFeedback();
  const payload = {
    message,
    session_id: el.sessionId.value.trim() || "ui-session",
    db_url: el.dbUrl.value.trim(),
    semantic_config_path: el.semanticPath.value.trim() || null,
    llm_provider: el.llmProvider.value,
    llm_model: el.llmModel.value.trim(),
  };

  setStatus("Thinking...", "busy");
  setInFlight(true);
  try {
    thinking.progress("conversation.wait", "Connecting to the analysis engine");
    const data = await streamChat(payload, (progressData) => {
      thinking.progress(progressData.stage, progressData.message);
    });
    thinking.stop(true);
    addMessage({ role: "agent", text: data.message, mode: data.mode, sql: data.sql || "" });
    el.modeValue.textContent = data.mode || "-";
    el.rowCountValue.textContent = String(data.row_count ?? 0);
    el.sqlText.textContent = data.sql || "-- No SQL returned";
    el.notesPath.textContent = data.notes_path ? `Notes: ${data.notes_path}` : "";

    renderTable(data.columns || [], data.rows || []);
    renderChart(data);
    setStatus("Ready", "ready");
  } catch (error) {
    thinking.stop(true);
    addMessage({ role: "agent", text: `Request failed: ${error.message}`, mode: "error" });
    setStatus("Error", "error");
  } finally {
    setInFlight(false);
  }
}

el.chatForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  if (isSubmitting) {
    return;
  }
  const message = el.chatInput.value.trim();
  if (!message) {
    return;
  }
  addMessage({ role: "user", text: message });
  el.chatInput.value = "";
  await submitMessage(message);
});

el.chips.forEach((chip) => {
  chip.addEventListener("click", () => {
    if (isSubmitting) {
      return;
    }
    const prompt = chip.getAttribute("data-prompt") || "";
    el.chatInput.value = prompt;
    el.chatInput.focus();
  });
});

addMessage({
  role: "agent",
  mode: "chat",
  text:
    "Session started. Ask a business question and I will return insights, SQL, chart preview, and saved notes path.",
});
