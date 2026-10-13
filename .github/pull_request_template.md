## What changed

## Checklist
- [ ] New DDL is a **new** numbered changeset (no edits to deployed ones) and is in `changelog-master.yml`
- [ ] Every changeset has a `--rollback`
- [ ] Non-bronze objects are schema-qualified (`SILVER.`, `GOLD.`, `AUDIT.`)
- [ ] Transformations are idempotent (MERGE / INSERT OVERWRITE)
- [ ] Tests added or updated; `pytest` passes locally
- [ ] No secrets or key files committed
