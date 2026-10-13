Snowflake AI Customer 360
Financial-services Customer 360 platform on Snowflake: CRM, accounts, transactions,
support tickets and products flowing Bronze → Silver → Gold, with Cortex AI/ML on top.
Architecture
Sources (CSV in `data/`) → Bronze (raw, via Python loader) → Silver (typed/cleaned)
→ Gold (marts) → AI/ML (Cortex Search, Analyst, RAG) → Consumption
Layer	How it is created
Schemas, tables	Liquibase (`sql/ddl/`), deployed by GitHub Actions
Bronze data	Python loader (`src/ingestion/`), PUT + COPY INTO
Ingestion audit	`BRONZE.INGESTION_FILE_LOG` (file name + SHA-256 hash)
> Liquibase only creates objects. **Data is loaded by the Python loader**, which the
> DEV and QA deploy workflows run after Liquibase.
Environments and branches
`dev` → `qa` → `main` (PROD). Each branch deploys to its GitHub Environment
(`dev`, `qa`, `prod`). PROD does not auto-load sample data.
Local setup
```bash
python -m venv .venv && source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt -r requirements-dev.txt
cp .env.example .env                                    # fill in credentials
pytest
```
Loading Bronze data
Run from the repo root (as a module — the package uses relative imports):
```bash
python -m src.ingestion.bronze_ingestion                # all sources
python -m src.ingestion.bronze_ingestion customer       # one source
python -m src.ingestion.bronze_ingestion --force        # reload after truncating a table
```
Rules the loader enforces:
Each source uploads to its own stage path (`@FINAI_BRONZE_INGEST_STAGE/<source>/`).
A file is skipped only if the same name and hash previously loaded >0 rows.
A COPY that loads 0 rows is an error (logged `FAILED`), never `SUCCESS`.
Troubleshooting: run `scripts/diagnose.sql` in Snowflake.
Database changes (Liquibase) — rules
Never edit a changeset that has been deployed. Add a new numbered file and
include it in `sql/ddl/changelog-master.yml`.
Always schema-qualify non-bronze objects (`SILVER.CUSTOMER`).
Every changeset needs a `--rollback`.
Non-prod recovery from checksum errors: `scripts/reset_nonprod.sql` (never PROD).
Secrets
Credentials live only in GitHub Environment secrets / local `.env`. Never commit keys.
If a key was committed, follow `scripts/untrack_secrets.sh`.