const { Plugin, Notice, normalizePath } = require("obsidian");

const DATA_PATH = "Sail Rust Book/_data/fragments.json";
const STORAGE_KEY = "sail-code-fragment-highlight";

class SailCodeFragmentsPlugin extends Plugin {
  async onload() {
    this.fragments = new Map();
    await this.loadFragments();

    this.registerMarkdownCodeBlockProcessor("sail-fragment", async (source, el, ctx) => {
      let payload;
      try {
        payload = JSON.parse(source.trim());
      } catch (error) {
        el.createEl("pre", { text: `Invalid sail-fragment payload: ${error.message}` });
        return;
      }
      const fragment = this.fragments.get(payload.id) || payload;
      const card = el.createDiv({ cls: "sail-fragment-card" });
      const title = card.createDiv({ cls: "sail-fragment-title" });
      title.createSpan({ text: fragment.id || "unknown fragment" });
      const meta = card.createDiv({ cls: "sail-fragment-meta" });
      meta.setText(`${fragment.sourcePath || fragment.source_path || ""}:${fragment.startLine || fragment.start_line || "?"}-${fragment.endLine || fragment.end_line || "?"}`);
      const button = card.createEl("button", { text: "Open code fragment" });
      button.addEventListener("click", async () => {
        await this.openFragment(fragment, ctx.sourcePath);
      });
    });

    this.addCommand({
      id: "open-sail-code-fragment",
      name: "Open Sail code fragment by ID",
      callback: async () => {
        new Notice("Use a rendered Sail fragment card and click Open code fragment.");
      },
    });

    this.registerEvent(this.app.workspace.on("file-open", () => {
      window.setTimeout(() => this.highlightRequestedFragment(), 250);
    }));
  }

  async loadFragments() {
    try {
      const text = await this.app.vault.adapter.read(normalizePath(DATA_PATH));
      for (const row of JSON.parse(text)) {
        this.fragments.set(row.id, {
          id: row.id,
          codeNote: row.code_note,
          heading: row.heading,
          sourcePath: row.source_path,
          startLine: row.start_line,
          endLine: row.end_line,
        });
      }
    } catch (error) {
      console.warn("sail-code-fragments: could not load fragments", error);
    }
  }

  async openFragment(fragment, sourcePath) {
    if (!fragment || !fragment.codeNote) {
      new Notice("No code note for this fragment.");
      return;
    }
    window.localStorage.setItem(STORAGE_KEY, fragment.id);
    const link = fragment.heading
      ? `${fragment.codeNote}#${fragment.heading}`
      : fragment.codeNote;
    await this.app.workspace.openLinkText(link, sourcePath || "");
    window.setTimeout(() => this.highlightRequestedFragment(), 350);
  }

  highlightRequestedFragment() {
    const id = window.localStorage.getItem(STORAGE_KEY);
    if (!id) return;
    document.querySelectorAll(".sail-fragment-highlight").forEach((el) => {
      el.classList.remove("sail-fragment-highlight");
    });
    const target = document.getElementById(id);
    if (!target) return;
    const section = target.closest(".markdown-preview-section") || target.parentElement;
    if (section) section.classList.add("sail-fragment-highlight");
    target.scrollIntoView({ behavior: "smooth", block: "center" });
  }
}

module.exports = SailCodeFragmentsPlugin;
