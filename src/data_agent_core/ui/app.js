const el = {
  chatForm: document.getElementById("chatForm"),
  chatInput: document.getElementById("chatInput"),
  chatLog: document.getElementById("chatLog"),
  sendBtn: document.getElementById("sendBtn"),
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

function setStatus(text, isError = false) {
  el.status.textContent = text;
  el.status.style.borderColor = isError ? "rgba(248, 113, 113, 0.5)" : "rgba(45, 212, 191, 0.4)";
  el.status.style.background = isError ? "rgba(248, 113, 113, 0.18)" : "rgba(45, 212, 191, 0.12)";
  el.status.style.color = isError ? "#ffd2d2" : "#bffef3";
}

function addMessage({ role, text, mode = "", sql = "" }) {
  const block = document.createElement("div");
  block.className = `msg ${role === "user" ? "user" : "agent"}`;

  const head = document.createElement("div");
  head.className = "msg-head";
  head.innerHTML = `<span>${role === "user" ? "You" : "Agent"}</span>${
    mode ? `<span class="mode-badge">${mode}</span>` : ""
  }`;
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

  el.chatLog.appendChild(block);
  el.chatLog.scrollTop = el.chatLog.scrollHeight;
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
  const payload = {
    message,
    session_id: el.sessionId.value.trim() || "ui-session",
    db_url: el.dbUrl.value.trim(),
    semantic_config_path: el.semanticPath.value.trim() || null,
    llm_provider: el.llmProvider.value,
    llm_model: el.llmModel.value.trim(),
  };

  setStatus("Running...");
  el.sendBtn.disabled = true;
  try {
    const response = await fetch("/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const data = await response.json();
    if (!response.ok) {
      const detail = data.detail ? JSON.stringify(data.detail) : JSON.stringify(data);
      throw new Error(detail);
    }

    addMessage({ role: "agent", text: data.message, mode: data.mode, sql: data.sql || "" });
    el.modeValue.textContent = data.mode || "-";
    el.rowCountValue.textContent = String(data.row_count ?? 0);
    el.sqlText.textContent = data.sql || "-- No SQL returned";
    el.notesPath.textContent = data.notes_path ? `Notes: ${data.notes_path}` : "";

    renderTable(data.columns || [], data.rows || []);
    renderChart(data);
    setStatus("Ready");
  } catch (error) {
    addMessage({ role: "agent", text: `Request failed: ${error.message}`, mode: "error" });
    setStatus("Error", true);
  } finally {
    el.sendBtn.disabled = false;
  }
}

el.chatForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const message = el.chatInput.value.trim();
  if (!message) {
    return;
  }
  addMessage({ role: "user", text: message });
  el.chatInput.value = "";
  await submitMessage(message);
});

document.querySelectorAll(".chip").forEach((chip) => {
  chip.addEventListener("click", () => {
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
