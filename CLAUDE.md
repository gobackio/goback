# goback

goback is an open-source, deduplicating backup tool: a Go library with a CLI (`cmd/goback`), an agent and a store server. Module `github.com/gobackio/goback`, Apache-2.0, public on GitHub.

## Scope

- This repository is the whole product: everything stays single-store and single-user. A feature that only makes sense for one particular deployment belongs outside it.
- No code, comment, test or doc names a downstream project, product or deployment. Tests use `example.com` hosts.
- Downstream needs are met with general library API, never special cases, and only after the user agreed to the change.
- The CLI is a library consumer: everything supported must be reachable from the library, and `cmd/` holds only flags and output.
- No GPL or AGPL dependencies.

## Design and decisions

- Design work goes into a doc under `docs/` (not committed) and is shared with the user on the phone. Open questions go on decision cards, and nothing a doc recommends is applied before the user decides.
- After each design doc, commit the code without asking. Pushing and tagging need the user's go.
- The database is only an index: losing it never loses backup data. Keys and anything a restore needs live in the store.
- goback backs up everything; never suggest exclude patterns as a fix.
- Use vetted libraries for crypto (Tink, age), never own constructions.
- Protocol changes require the new form: no fallbacks for older servers or agents.
- Telemetry is OpenTelemetry only.
- Progress bars measure the real work (bytes scanned and sent) and every phase shows its own status.

## Code

- Comments are lava. Comment only what the code can't say; never history or conversation. Every exported identifier has a short doc comment saying what it is and how to use it, starting with its name. Prefer `ExampleXxx` over prose.
- Type mappings go through `github.com/ligustah/mapper` codegen, never hand-written field copying.
- No names that read as the wrong shape, and no leftovers from old designs.
- Tests: no assertions on UI wording, no tests of what a dependency guarantees, listeners bind `127.0.0.1`.
- GC and retention changes are checked against the TLA+ specs in `spec/`. TLC runs are time-boxed to about ten minutes: a small exhaustive model plus a mutation that must fail.

## Working here

- Run go with `GOMAXPROCS=4 GOFLAGS=-p=2`, one `go test` at a time, on the touched packages. At most two or three agents at once.
- Files are CRLF (`autocrlf=true`). Convert added lines with `perl -pi -e 's/(?<!\r)\n/\r\n/'`. Migrations under `index/sql/migrations` and `*.sh` are LF.
- Commit messages are one present-tense prose paragraph saying what the code now does, a blank line, then the attribution trailers. History is linear and tags are lightweight.
- Never commit `.claude/`, `go.work`, `*.exe`, `spec/GC_TTrace_*` or anything with secrets. Never open `cloudflare.txt`, `.env` files or key files.
- Never use bare `git stash` or `git add -N .`. Leave existing stashes and the `goback-legacy` worktree alone.
