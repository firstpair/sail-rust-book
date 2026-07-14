## FirstPair Book Delivery

`FIRSTPAIR.md` is the required contract for this repository's unified book
build and FirstPair library deployment. Read and maintain it before changing or
delivering the book; it owns the catalog slug, shelf, and all source-side
handoff guidance. The shared implementation and authoritative operational rules
live in `~/src/firstpair`. Do not duplicate that deployment procedure here.

Before running `sail-rust-book/scripts/build-obsidian-vault.py`, editing files
under `sail-rust-book/book/dist-obsidian/`, zipping a vault, or otherwise
touching a vault programmatically, ask the user to close the vault in Obsidian
and wait for confirmation. Do not regenerate or mutate a vault while it is open
in Obsidian; the app can rewrite workspace, plugin, and index files and race
generated output, leaving a poisoned vault. After confirmation, regenerate from
source and run `sail-rust-book/scripts/check-obsidian-vault.py` before
publishing.

## Blog And Announcement Ownership

Project-owned announcements, blog posts, textpacks, and their assets live under
`sail-rust-book/blog/` beside `sail-rust-book/book/`. FirstPair may package,
deliver, index, or host derived outputs, but it must not become the durable
source of Sail Rust Book editorial content.

## Imported Claude Cowork project instructions

building a book in md, then pandoc and typst to pdf
