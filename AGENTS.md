# Repository Guidelines

## Project Structure & Module Organization
Core runtime modules live at the repository root: `app.py` (Q&A backend + Gradio endpoint), `bot.py` (Discord interactions), `docstore.py` (MongoDB access), `vecstore.py` (FAISS index), and `prompts.py` (prompt templates). ETL logic is in `etl/` (`pdfs.py`, `markdown.py`, `videos.py`, shared helpers). Operational scripts are in `tasks/`, setup docs and screenshots in `setup/`, and source corpus inputs in `data/`.

## Build, Test, and Development Commands
Use `make` targets as the primary interface:
- `make dev-environment`: install runtime + dev tooling.
- `make environment`: install runtime dependencies only.
- `make help`: list available workflows.
- `make document-store`: run ETL and load MongoDB collection.
- `make vector-index`: build FAISS index from document store.
- `make serve-backend` / `make serve-frontend`: run hot-reloading Modal services.
- `make backend` / `make frontend`: deploy services to Modal.
- `make cli-query QUERY="..."`: smoke-test retrieval and answer generation.

## Coding Style & Naming Conventions
Target Python 3.10 (`.python-version`). Use 4-space indentation and PEP 8 naming (`snake_case` for functions/modules, `UPPER_SNAKE_CASE` for constants). Run quality checks before opening a PR:
- `ruff check .`
- `black --check .`
- `pre-commit run --all-files`

## Testing Guidelines
There is no dedicated `tests/` suite in this repo yet. Treat linting and workflow smoke tests as required validation:
1. Run static checks (`ruff`, `black`, `pre-commit`).
2. Run an end-to-end query via `make cli-query QUERY="What is chain-of-thought prompting?"`.
3. If ETL or retrieval code changes, re-run `make document-store` and `make vector-index` in a dev environment.

## Commit & Pull Request Guidelines
Follow the existing git style: short, imperative, lowercase summaries (for example, `updates to latest modal client version`, `resolves modal deprecation warnings`), with issue references when relevant (`#73`). Keep commits focused by concern (ETL, backend, bot, infra). PRs should include:
- what changed and why,
- impacted modules/files,
- commands run for validation,
- linked issues,
- screenshots/log snippets for UI or deployment-affecting changes.

## Security & Configuration Tips
Never commit secrets. Copy `.env.example` to `.env` or `.env.dev`, and inject credentials through `make secrets` / `make frontend-secrets`. Validate Modal auth with `make modal-auth` before deploy targets.
