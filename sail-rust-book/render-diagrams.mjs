import fs from "node:fs";
import path from "node:path";

const root = ".codex-artifacts/sail-rust-arrow-datafusion-book";
const diagramDir = path.join(root, "diagrams");
fs.mkdirSync(diagramDir, { recursive: true });

const files = fs
  .readdirSync(root)
  .filter((name) => /^\d\d-.*\.md$/.test(name))
  .sort();

const escapeXml = (value) =>
  String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");

const FLOW_NODE_FONT_SIZE = 26;
const FLOW_GROUP_FONT_SIZE = 24;
const FLOW_EDGE_FONT_SIZE = 22;
const FLOW_LINE_HEIGHT = 32;
const FLOW_TEXT_Y_OFFSET = 16;
const FLOW_TEXT_CHAR_WIDTH = 12;
const FLOW_NODE_MIN_WIDTH = 170;
const FLOW_NODE_MAX_WIDTH = 300;
const FLOW_NODE_PADDING_X = 40;
const FLOW_NODE_PADDING_Y = 26;
const SEQUENCE_FONT_SIZE = 24;
const SEQUENCE_MESSAGE_FONT_SIZE = 22;
const SEQUENCE_PARTICIPANT_WIDTH = 190;
const DIAGRAM_PADDING = 36;

function wrapText(text, maxChars = 24) {
  const words = String(text).replace(/\s+/g, " ").trim().split(" ");
  const lines = [];
  let current = "";
  for (const word of words) {
    if (!current) {
      current = word;
    } else if ((current + " " + word).length <= maxChars) {
      current += " " + word;
    } else {
      lines.push(current);
      current = word;
    }
  }
  if (current) lines.push(current);
  return lines.length ? lines : [""];
}

function nodeSvg(node) {
  measureNode(node);
  const lines = wrapText(node.label, 18);
  const width = node.width;
  const height = node.height;
  const rx = node.shape === "decision" ? 10 : 8;
  const fill = node.shape === "decision" ? "#fff6d9" : "#f6f8fb";
  const stroke = node.shape === "decision" ? "#c58c00" : "#496a9a";
  const tspans = lines
    .map((line, index) => {
      const dy = index === 0 ? 0 : FLOW_LINE_HEIGHT;
      return `<tspan x="${node.x}" dy="${dy}">${escapeXml(line)}</tspan>`;
    })
    .join("");
  return `<rect x="${node.x - width / 2}" y="${node.y - height / 2}" width="${width}" height="${height}" rx="${rx}" fill="${fill}" stroke="${stroke}" stroke-width="1.5"/>
<text x="${node.x}" y="${node.y - (lines.length - 1) * FLOW_TEXT_Y_OFFSET}" text-anchor="middle" dominant-baseline="middle" font-family="Inter, Helvetica, Arial, sans-serif" font-size="${FLOW_NODE_FONT_SIZE}" fill="#1f2937">${tspans}</text>`;
}

function measureNode(node) {
  if (node.width && node.height) return;
  const lines = wrapText(node.label, 18);
  const width = Math.max(
    FLOW_NODE_MIN_WIDTH,
    Math.min(
      FLOW_NODE_MAX_WIDTH,
      Math.max(...lines.map((x) => x.length)) * FLOW_TEXT_CHAR_WIDTH + FLOW_NODE_PADDING_X,
    ),
  );
  const height = FLOW_NODE_PADDING_Y + lines.length * FLOW_LINE_HEIGHT;
  node.width = width;
  node.height = height;
}

function parseNode(raw, nodes) {
  let text = raw.trim().replace(/;$/, "");
  text = text.replace(/^\|(?:\"([^\"]+)\"|([^|]+))\|\s*/, "");
  const match = text.match(/^([A-Za-z][A-Za-z0-9_]*)(?:\s*(\[[\s\S]*?\]|\{[\s\S]*?\}))?/);
  if (!match) return null;
  const id = match[1];
  let label = id;
  let shape = "box";
  const suffix = match[2];
  if (suffix) {
    shape = suffix.startsWith("{") ? "decision" : "box";
    const labelMatch = suffix.match(/^[\[\{]\s*"([\s\S]*?)"\s*[\]\}]$/);
    label = labelMatch ? labelMatch[1] : suffix.slice(1, -1);
  }
  if (!nodes.has(id)) nodes.set(id, { id, label, shape });
  else {
    const node = nodes.get(id);
    if (label !== id) node.label = label;
    if (shape !== "box") node.shape = shape;
  }
  return id;
}

function parseFlowchart(source) {
  const lines = source.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  const first = lines.shift() ?? "flowchart TB";
  const direction = /\bLR\b/.test(first) ? "LR" : "TB";
  const nodes = new Map();
  const edges = [];
  const groups = [];
  const stack = [];

  for (const line of lines) {
    if (line.startsWith("subgraph ")) {
      const rest = line.slice("subgraph ".length).trim();
      let id = `group${groups.length + 1}`;
      let label = rest.replace(/^"|"$/g, "");
      const idLabel = rest.match(/^([A-Za-z][A-Za-z0-9_]*)\["([\s\S]*?)"\]$/);
      if (idLabel) {
        id = idLabel[1];
        label = idLabel[2];
      }
      const group = { id, label, nodes: [] };
      groups.push(group);
      stack.push(group);
      continue;
    }
    if (line === "end") {
      stack.pop();
      continue;
    }
    if (line.includes("-->")) {
      const [leftRaw, rightRawFull] = line.split("-->");
      const from = parseNode(leftRaw, nodes);
      let label = "";
      let rightRaw = rightRawFull.trim();
      const labelMatch = rightRaw.match(/^\|(?:\"([^\"]+)\"|([^|]+))\|\s*(.*)$/);
      if (labelMatch) {
        label = labelMatch[1] ?? labelMatch[2] ?? "";
        rightRaw = labelMatch[3];
      }
      const to = parseNode(rightRaw, nodes);
      if (from && to) edges.push({ from, to, label });
      for (const id of [from, to]) {
        if (id && stack.length) {
          const group = stack.at(-1);
          if (!group.nodes.includes(id)) group.nodes.push(id);
        }
      }
    } else {
      const id = parseNode(line, nodes);
      if (id && stack.length) {
        const group = stack.at(-1);
        if (!group.nodes.includes(id)) group.nodes.push(id);
      }
    }
  }

  const ids = [...nodes.keys()];
  const rank = Object.fromEntries(ids.map((id) => [id, undefined]));
  for (const edge of edges) {
    if (rank[edge.from] == null) rank[edge.from] = 0;
    if (rank[edge.to] == null) rank[edge.to] = rank[edge.from] + 1;
  }
  for (const id of ids) {
    if (rank[id] == null) rank[id] = 0;
  }
  const byRank = new Map();
  for (const id of ids) {
    const r = rank[id] ?? 0;
    if (!byRank.has(r)) byRank.set(r, []);
    byRank.get(r).push(id);
  }

  const columnGap = 255;
  const rowGap = 130;
  const bandGap = 215;
  const margin = 90;
  for (const [r, groupIds] of byRank) {
    groupIds.forEach((id, index) => {
      const node = nodes.get(id);
      if (direction === "LR") {
        const maxColumns = 5;
        const column = r % maxColumns;
        const band = Math.floor(r / maxColumns);
        node.x = margin + column * columnGap;
        node.y = margin + band * bandGap + index * rowGap;
      } else {
        node.x = margin + index * columnGap;
        node.y = margin + r * rowGap;
      }
    });
  }
  for (const id of ids) measureNode(nodes.get(id));

  const bounds = {
    left: Math.min(...ids.map((id) => nodes.get(id).x - nodes.get(id).width / 2)),
    right: Math.max(...ids.map((id) => nodes.get(id).x + nodes.get(id).width / 2)),
    top: Math.min(...ids.map((id) => nodes.get(id).y - nodes.get(id).height / 2)),
    bottom: Math.max(...ids.map((id) => nodes.get(id).y + nodes.get(id).height / 2)),
  };
  const shiftX = DIAGRAM_PADDING - bounds.left;
  const shiftY = DIAGRAM_PADDING - bounds.top;
  for (const id of ids) {
    const node = nodes.get(id);
    node.x += shiftX;
    node.y += shiftY;
  }
  const width = Math.max(420, bounds.right - bounds.left + DIAGRAM_PADDING * 2);
  const height = Math.max(220, bounds.bottom - bounds.top + DIAGRAM_PADDING * 2);

  const groupSvg = groups
    .filter((group) => group.nodes.length > 0)
    .map((group) => {
      const ns = group.nodes.map((id) => nodes.get(id)).filter(Boolean);
      const minX = Math.min(...ns.map((n) => n.x - 95)) - 18;
      const maxX = Math.max(...ns.map((n) => n.x + 95)) + 18;
      const minY = Math.min(...ns.map((n) => n.y - 35)) - 26;
      const maxY = Math.max(...ns.map((n) => n.y + 35)) + 18;
      return `<rect x="${minX}" y="${minY}" width="${maxX - minX}" height="${maxY - minY}" rx="10" fill="#eef6ff" stroke="#9bc3e6" stroke-dasharray="5 4"/>
<text x="${minX + 12}" y="${minY + 24}" font-family="Inter, Helvetica, Arial, sans-serif" font-size="${FLOW_GROUP_FONT_SIZE}" font-weight="700" fill="#315a86">${escapeXml(group.label)}</text>`;
    })
    .join("\n");

  const edgeSvg = edges
    .map((edge) => {
      const from = nodes.get(edge.from);
      const to = nodes.get(edge.to);
      if (!from || !to) return "";
      const sx = from.x + (direction === "LR" ? from.width / 2 : 0);
      const sy = from.y + (direction === "LR" ? 0 : from.height / 2);
      const tx = to.x - (direction === "LR" ? to.width / 2 : 0);
      const ty = to.y - (direction === "LR" ? 0 : to.height / 2);
      const mx = (sx + tx) / 2;
      const my = (sy + ty) / 2;
      const label = edge.label
        ? `<text x="${mx}" y="${my - 8}" text-anchor="middle" font-family="Inter, Helvetica, Arial, sans-serif" font-size="${FLOW_EDGE_FONT_SIZE}" fill="#4b5563">${escapeXml(edge.label)}</text>`
        : "";
      return `<line x1="${sx}" y1="${sy}" x2="${tx}" y2="${ty}" stroke="#5f6f85" stroke-width="1.5" marker-end="url(#arrow)"/>${label}`;
    })
    .join("\n");

  return wrapSvg(width, height, `${groupSvg}\n${edgeSvg}\n${ids.map((id) => nodeSvg(nodes.get(id))).join("\n")}`);
}

function parseSequence(source) {
  const lines = source.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  lines.shift();
  const participants = [];
  const labels = new Map();
  const messages = [];

  for (const line of lines) {
    const p = line.match(/^participant\s+(\S+)(?:\s+as\s+([\s\S]+))?$/);
    if (p) {
      const id = p[1];
      if (!participants.includes(id)) participants.push(id);
      labels.set(id, p[2] ?? id);
      continue;
    }
    const msg = line.match(/^(\S+?)(-+>>)(\S+?):\s*([\s\S]+)$/);
    if (msg) {
      const [, from, arrow, to, label] = msg;
      for (const id of [from, to]) {
        if (!participants.includes(id)) participants.push(id);
        if (!labels.has(id)) labels.set(id, id);
      }
      messages.push({ from, to, label, dashed: arrow.startsWith("--") });
    }
  }

  const gap = 240;
  const margin = 100;
  const top = 70;
  const messageGap = 76;
  const bottom = top + messages.length * messageGap + 90;
  const width = Math.max(420, margin * 2 + Math.max(0, participants.length - 1) * gap);
  const height = bottom + 55;
  const xs = new Map(participants.map((id, index) => [id, margin + index * gap]));

  const participantSvg = participants
    .map((id) => {
      const x = xs.get(id);
      const label = escapeXml(labels.get(id));
      return `<rect x="${x - SEQUENCE_PARTICIPANT_WIDTH / 2}" y="${top - 44}" width="${SEQUENCE_PARTICIPANT_WIDTH}" height="44" rx="8" fill="#f6f8fb" stroke="#496a9a"/>
<text x="${x}" y="${top - 16}" text-anchor="middle" font-family="Inter, Helvetica, Arial, sans-serif" font-size="${SEQUENCE_FONT_SIZE}" fill="#1f2937">${label}</text>
<line x1="${x}" y1="${top}" x2="${x}" y2="${bottom}" stroke="#b5bdc8" stroke-dasharray="4 4"/>
<rect x="${x - SEQUENCE_PARTICIPANT_WIDTH / 2}" y="${bottom}" width="${SEQUENCE_PARTICIPANT_WIDTH}" height="44" rx="8" fill="#f6f8fb" stroke="#496a9a"/>
<text x="${x}" y="${bottom + 29}" text-anchor="middle" font-family="Inter, Helvetica, Arial, sans-serif" font-size="${SEQUENCE_FONT_SIZE}" fill="#1f2937">${label}</text>`;
    })
    .join("\n");

  const messageSvg = messages
    .map((msg, index) => {
      const y = top + 35 + index * messageGap;
      const from = xs.get(msg.from);
      const to = xs.get(msg.to);
      if (from === to) {
        const x = from;
        return `<path d="M ${x} ${y} C ${x + 70} ${y}, ${x + 70} ${y + 28}, ${x} ${y + 28}" fill="none" stroke="#5f6f85" stroke-width="1.5" marker-end="url(#arrow)"/>
<text x="${x + 74}" y="${y + 20}" font-family="Inter, Helvetica, Arial, sans-serif" font-size="${SEQUENCE_MESSAGE_FONT_SIZE}" fill="#374151">${escapeXml(msg.label)}</text>`;
      }
      const start = from < to ? from + 12 : from - 12;
      const end = from < to ? to - 12 : to + 12;
      const dash = msg.dashed ? 'stroke-dasharray="5 4"' : "";
      const tx = (start + end) / 2;
      return `<line x1="${start}" y1="${y}" x2="${end}" y2="${y}" stroke="#5f6f85" stroke-width="1.5" ${dash} marker-end="url(#arrow)"/>
<text x="${tx}" y="${y - 10}" text-anchor="middle" font-family="Inter, Helvetica, Arial, sans-serif" font-size="${SEQUENCE_MESSAGE_FONT_SIZE}" fill="#374151">${escapeXml(msg.label)}</text>`;
    })
    .join("\n");

  return wrapSvg(width, height, `${participantSvg}\n${messageSvg}`);
}

function wrapSvg(width, height, body) {
  const displayWidth = Math.min(width, 900);
  const displayHeight = Math.round(height * (displayWidth / width));
  return `<svg xmlns="http://www.w3.org/2000/svg" width="${displayWidth}" height="${displayHeight}" viewBox="0 0 ${width} ${height}">
<defs>
  <marker id="arrow" markerWidth="10" markerHeight="10" refX="8" refY="3" orient="auto" markerUnits="strokeWidth">
    <path d="M0,0 L0,6 L8,3 z" fill="#5f6f85"/>
  </marker>
</defs>
<rect x="0" y="0" width="${width}" height="${height}" fill="white"/>
${body}
</svg>`;
}

function render(source) {
  if (/^\s*sequenceDiagram\b/.test(source)) return parseSequence(source);
  return parseFlowchart(source);
}

function isMermaidStart(line) {
  const value = line.trim();
  return /^flowchart\s+(LR|TD|TB)/.test(value) || value === "sequenceDiagram";
}

function isMermaidLine(line, mode) {
  const value = line.replace(/^\f/, "").trim();
  if (!value) return false;
  if (isMermaidStart(value)) return true;
  if (mode === "sequence") {
    return /^participant\s+\S+/.test(value) || /^\S+[-=]*>>\S+:/.test(value);
  }
  return (
    /^subgraph\b/.test(value) ||
    value === "end" ||
    /-->/.test(value) ||
    /^[A-Za-z][A-Za-z0-9_]*(\[|\{)/.test(value) ||
    /^[A-Za-z][A-Za-z0-9_]*$/.test(value)
  );
}

function nextNonNoiseLine(lines, start) {
  for (let index = start; index < lines.length; index += 1) {
    const value = lines[index].replace(/^\f/, "").trim();
    if (!value || /^\d+$/.test(value)) continue;
    return value;
  }
  return "";
}

function extractMermaidBlocksFromText(text) {
  const lines = text.split(/\r?\n/).map((line) => line.replace(/^\f/, ""));
  const blocks = [];
  for (let index = 0; index < lines.length; index += 1) {
    if (!isMermaidStart(lines[index])) continue;
    const mode = lines[index].trim() === "sequenceDiagram" ? "sequence" : "flow";
    const block = [lines[index].trim()];
    index += 1;
    for (; index < lines.length; index += 1) {
      const value = lines[index].replace(/^\f/, "").trimEnd();
      if (!value.trim() || /^\d+$/.test(value.trim())) {
        const next = nextNonNoiseLine(lines, index + 1);
        if (next && isMermaidLine(next, mode)) continue;
        break;
      }
      if (isMermaidLine(value, mode)) {
        block.push(value.trim());
      } else {
        index -= 1;
        break;
      }
    }
    blocks.push(block.join("\n"));
  }
  return blocks;
}

function diagramNamesFromMarkdown() {
  const names = [];
  for (const file of files) {
    const content = fs.readFileSync(path.join(root, file), "utf8");
    const matches = content.matchAll(/!\[[^\]]+\]\(diagrams\/([^)]+\.svg)\)/g);
    for (const match of matches) names.push(match[1]);
  }
  return names;
}

const pdfTextPath = process.argv[2];
if (pdfTextPath) {
  const blocks = extractMermaidBlocksFromText(fs.readFileSync(pdfTextPath, "utf8"));
  const names = diagramNamesFromMarkdown();
  if (blocks.length !== names.length) {
    throw new Error(`Expected ${names.length} diagrams from markdown, extracted ${blocks.length} from ${pdfTextPath}`);
  }
  blocks.forEach((source, index) => {
    fs.writeFileSync(path.join(diagramDir, names[index]), render(source));
  });
  console.log(`Rendered ${blocks.length} Mermaid diagrams from ${pdfTextPath}`);
  process.exit(0);
}

let globalIndex = 0;
for (const file of files) {
  const fullPath = path.join(root, file);
  let content = fs.readFileSync(fullPath, "utf8");
  let localIndex = 0;
  content = content.replace(/```mermaid\n([\s\S]*?)\n```/g, (_, source) => {
    globalIndex += 1;
    localIndex += 1;
    const chapter = file.slice(0, 2);
    const name = `${chapter}-diagram-${String(localIndex).padStart(2, "0")}.svg`;
    const svg = render(source);
    fs.writeFileSync(path.join(diagramDir, name), svg);
    const kind = /^\s*sequenceDiagram\b/.test(source) ? "Sequence diagram" : "Flowchart";
    return `![${kind} ${chapter}.${localIndex}](diagrams/${name})`;
  });
  fs.writeFileSync(fullPath, content);
}

console.log(`Rendered ${globalIndex} Mermaid diagrams to ${diagramDir}`);
