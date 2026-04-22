const els = {
  message: document.getElementById("message"),
  broker: document.getElementById("broker"),
  send: document.getElementById("send"),
  logs: document.getElementById("logs"),
  consumerEnabled: document.getElementById("consumerEnabled"),
  clear: document.getElementById("clear"),
  status: document.getElementById("status"),
  sumKafka: document.getElementById("sumKafka"),
  sumRabbit: document.getElementById("sumRabbit"),
  sumRedis: document.getElementById("sumRedis"),
};

async function api(path, options) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`${res.status} ${res.statusText}: ${text}`);
  }
  return await res.json();
}

function esc(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function brokerBadge(broker) {
  const b = String(broker || "");
  const cls = b === "rabbitmq" ? "rabbitmq" : b;
  return `<span class="badge ${esc(cls)}">${esc(b)}</span>`;
}

function fmtLatency(latencyMs) {
  if (latencyMs === null || latencyMs === undefined) return "-";
  return `${latencyMs} ms`;
}

function renderLogs(events) {
  if (!els.logs) return;
  const header =
    `<div class="logRow logHead">` +
    `<div>time</div>` +
    `<div>broker</div>` +
    `<div>event</div>` +
    `<div>message</div>` +
    `<div style="text-align:right">latency</div>` +
    `</div>`;

  const rows = events
    .slice()
    .reverse()
    .map((e) => {
      const ts = new Date(e.ts).toLocaleTimeString();
      const type = String(e.type || "");
      const id = e.id ? ` • id=${String(e.id).slice(0, 8)}` : "";
      const msg = `${e.message ?? ""}${id}`;
      return (
        `<div class="logRow">` +
        `<div>${esc(ts)}</div>` +
        `<div>${brokerBadge(e.broker)}</div>` +
        `<div class="type ${esc(type)}">${esc(type)}</div>` +
        `<div class="msg">${esc(msg)}</div>` +
        `<div class="lat">${esc(fmtLatency(e.latencyMs))}</div>` +
        `</div>`
      );
    })
    .join("");

  els.logs.innerHTML = header + rows;
  els.logs.scrollTop = 0;
}

function calcSummary(events, broker) {
  let sent = 0;
  let received = 0;
  let latSum = 0;
  let latCount = 0;

  for (const e of events) {
    if (e.broker !== broker) continue;
    if (e.type === "SENT") sent++;
    if (e.type === "RECEIVED") {
      received++;
      if (typeof e.latencyMs === "number") {
        latSum += e.latencyMs;
        latCount++;
      }
    }
  }

  const avg = latCount ? Math.round(latSum / latCount) : null;
  return { sent, received, avg };
}

function setSummary(cardEl, summary) {
  if (!cardEl) return;
  cardEl.querySelector('[data-k="sent"]').textContent = String(summary.sent);
  cardEl.querySelector('[data-k="received"]').textContent = String(summary.received);
  cardEl.querySelector('[data-k="avg"]').textContent = summary.avg === null ? "-" : `${summary.avg} ms`;
}

let lastRenderedCount = 0;
async function refresh() {
  try {
    const [logs, consumer] = await Promise.all([api("/logs"), api("/consumer")]);
    if (els.consumerEnabled) els.consumerEnabled.checked = !!consumer.enabled;
    if (logs.length !== lastRenderedCount) {
      renderLogs(logs);
      setSummary(els.sumKafka, calcSummary(logs, "kafka"));
      setSummary(els.sumRabbit, calcSummary(logs, "rabbitmq"));
      setSummary(els.sumRedis, calcSummary(logs, "redis"));
      lastRenderedCount = logs.length;
    }
    if (els.status) els.status.textContent = `events=${logs.length}`;
  } catch (e) {
    if (els.status) els.status.textContent = `UI error: ${String(e.message || e)}`;
  }
}

els.send.addEventListener("click", async () => {
  const message = els.message.value.trim();
  const broker = els.broker.value;
  if (!message) return;
  els.send.disabled = true;
  try {
    await api("/send", { method: "POST", body: JSON.stringify({ message, broker }) });
    els.message.value = "";
    await refresh();
  } catch (e) {
    alert(String(e.message || e));
  } finally {
    els.send.disabled = false;
  }
});

els.message.addEventListener("keydown", (e) => {
  if (e.key === "Enter") els.send.click();
});

els.consumerEnabled.addEventListener("change", async () => {
  const enabled = els.consumerEnabled.checked;
  try {
    await api("/consumer", { method: "POST", body: JSON.stringify({ enabled }) });
    await refresh();
  } catch (e) {
    alert(String(e.message || e));
    await refresh();
  }
});

els.clear.addEventListener("click", async () => {
  try {
    await api("/logs/clear", { method: "POST", body: "{}" });
    lastRenderedCount = 0;
    await refresh();
  } catch (e) {
    alert(String(e.message || e));
  }
});

refresh();
setInterval(refresh, 1000);

