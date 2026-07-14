# Sail Rust Book Obsidian Vault

The Sail Rust Book vault is a generated reader and code-navigation format for
*Sail Rust Book: The Rust, Arrow, and DataFusion Guide*. It collocates the book
chapters with Markdown notes for the current Sail codebase and generated code
fragments.

## Build

```sh
cd "$HOME/src/book-sources/sail-rust-book"
python3 sail-rust-book/scripts/build-obsidian-vault.py
python3 sail-rust-book/scripts/check-obsidian-vault.py \
  "sail-rust-book/book/dist-obsidian/Sail Rust Book Vault"
```

Set `SAIL_CODE_ROOT` or pass `--sail-root` to build from another Sail checkout.

## Vault Map

```text
Sail Rust Book Vault/
  Home.md
  Sail Rust Book/
    Book.md
    Chapters/
    Code/
    Crates/
    Fragments/
    Indices/
    Subsystems/
    _data/
  .obsidian/plugins/sail-code-fragments/
```

## Navigation

Open `Home.md`, then `Sail Rust Book/Book.md`. Chapter notes include generated
fragment cards. With the bundled `sail-code-fragments` plugin enabled, clicking
`Open code fragment` opens the collocated code-file note and highlights the
selected fragment area.

The vault is generated from source. Do not hand-edit generated notes; update the
book sources, current Sail checkout, generator, or plugin, then rebuild.
