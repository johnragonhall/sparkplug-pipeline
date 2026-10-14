"use strict";

const css = (name) => getComputedStyle(document.documentElement).getPropertyValue(name).trim();
const fmt = new Intl.NumberFormat("en-US");
const charts = {};
const ZWSP = String.fromCharCode(0x200b);
// Axis ticks land on whole minutes: 1, 2, 5, 10, 15 or 30 minutes apart.
const MINUTE_STEPS = [60, 120, 300, 600, 900, 1800];

async function getJSON(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url}: ${res.status}`);
  return res.json();
}

function el(tag, attrs = {}, text) {
  const node = document.createElement(tag);
  Object.assign(node, attrs);
  if (text !== undefined) node.textContent = text;
  return node;
}

function renderTiles(c) {
  const tiles = [
    ["Messages received", c.messages_received],
    ["Samples stored", c.samples_inserted],
    ["Duplicates dropped", c.samples_duplicate],
    ["Late samples stored", c.samples_late_inserted],
    ["Dead letters", c.dead_letters],
    ["Rollup refreshes", c.rollup_rows_refreshed],
  ];
  const box = document.getElementById("tiles");
  box.replaceChildren(
    ...tiles.map(([label, value]) => {
      const t = el("div", { className: "tile" });
      t.append(el("div", { className: "label" }, label), el("div", { className: "value" }, fmt.format(value ?? 0)));
      return t;
    }),
  );
}

function renderTable(id, rows, cols) {
  const body = document.querySelector(`#${id} tbody`);
  body.replaceChildren(
    ...rows.map((r) => {
      const tr = el("tr");
      cols.forEach((c) => tr.append(el("td", {}, String(typeof c === "function" ? c(r) : r[c]))));
      return tr;
    }),
  );
}

function chartWidth(id) {
  return Math.max(280, document.getElementById(id).clientWidth);
}

function drawAvg(data) {
  const devices = Object.keys(data.avg);
  const colors = [css("--s1"), css("--s2"), css("--s3")];
  const opts = {
    width: chartWidth("chart-avg"),
    height: 280,
    series: [
      { label: "Bucket (UTC)", value: (u, v) => (v == null ? "" : new Date(v * 1000).toISOString().slice(11, 16)) },
      ...devices.map((d, i) => ({ label: d, stroke: colors[i % colors.length], width: 2, spanGaps: false })),
    ],
    axes: [
      { stroke: css("--muted"), grid: { stroke: css("--border") }, incrs: MINUTE_STEPS, values: (u, ticks) => ticks.map((t) => new Date(t * 1000).toISOString().slice(11, 16)) },
      { stroke: css("--muted"), grid: { stroke: css("--border") } },
    ],
    scales: { x: { time: false } },
  };
  charts.avg?.destroy();
  charts.avg = new uPlot(opts, [data.buckets, ...devices.map((d) => data.avg[d])], document.getElementById("chart-avg"));
}

function drawLate(data) {
  const bars = uPlot.paths.bars({ size: [0.7, 40] });
  const opts = {
    width: chartWidth("chart-late"),
    height: 200,
    series: [
      { label: "Bucket (UTC)", value: (u, v) => (v == null ? "" : new Date(v * 1000).toISOString().slice(11, 16)) },
      { label: "Total samples", stroke: css("--ontime"), fill: css("--ontime"), paths: bars, points: { show: false } },
      { label: "Late samples", stroke: css("--late"), fill: css("--late"), paths: bars, points: { show: false } },
    ],
    axes: [
      { stroke: css("--muted"), grid: { show: false }, incrs: MINUTE_STEPS, values: (u, ticks) => ticks.map((t) => new Date(t * 1000).toISOString().slice(11, 16)) },
      { stroke: css("--muted"), grid: { stroke: css("--border") } },
    ],
    scales: { x: { time: false } },
  };
  charts.late?.destroy();
  // Late bars are drawn over the total bars, so the orange part is the late share.
  charts.late = new uPlot(opts, [data.buckets, data.total, data.late], document.getElementById("chart-late"));
}

async function loadSeries(metric) {
  const data = await getJSON(`/api/series?metric=${encodeURIComponent(metric)}`);
  drawAvg(data);
  drawLate(data);
}

async function main() {
  const [summary, dead] = await Promise.all([getJSON("/api/summary"), getJSON("/api/dead-letters?limit=50")]);
  renderTiles(summary.counters);
  // A zero-width space after each "/" lets long topics wrap at level boundaries.
  renderTable("dead-letters", dead, ["reason", (r) => r.topic.replaceAll("/", "/" + ZWSP), "detail"]);
  document.getElementById("event-counts").replaceChildren(
    ...summary.events.map((e) => {
      const chip = el("span", { className: "chip" }, `${e.event} `);
      chip.append(el("b", {}, fmt.format(e.count)));
      return chip;
    }),
  );
  renderTable("schemas", summary.schemas, [
    "device",
    (r) => `v${r.version}`,
    (r) => JSON.parse(r.metrics).map((m) => m.name).join(", "),
  ]);
  const select = document.getElementById("metric");
  select.replaceChildren(...summary.metrics.map((m) => el("option", { value: m }, m)));
  if (summary.metrics.includes("temperature")) select.value = "temperature";
  select.addEventListener("change", () => loadSeries(select.value));
  if (select.value) await loadSeries(select.value);
  window.addEventListener("resize", () => {
    for (const [key, chart] of Object.entries(charts)) chart.setSize({ width: chartWidth(`chart-${key}`), height: chart.height });
  });
  document.body.dataset.ready = "true";
}

main().catch((err) => {
  document.body.prepend(el("p", { className: "note" }, `Could not load data: ${err.message}`));
});
