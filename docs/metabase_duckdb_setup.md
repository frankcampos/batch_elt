# Adding Metabase (with DuckDB) to the Stack — What We Did & Why

A walkthrough of how we wired **Metabase** into the Docker stack so it can read
our **DuckDB** data, including the dead ends and the fix that worked. Written so
you can review the reasoning later.

---

## 1. The goal

Run Metabase (a BI / dashboard tool) as a container in `docker-compose.yaml`,
connected to our local DuckDB database (`duckdb/data.duckdb`) so we can build
dashboards on top of the pipeline output.

## 2. The pieces involved

| File | Role |
| --- | --- |
| `metabase.dockerfile.yaml` | Builds a custom Metabase image that includes the DuckDB driver |
| `docker-compose.yaml` | Defines the `metabase` service, ports, volumes |
| `duckdb/data.duckdb` | The DuckDB database Metabase reads (mounted into the container) |

Metabase does **not** ship with a DuckDB driver, so we have to add a
**community driver** (a `.jar` plugin) from
`motherduckdb/metabase_duckdb_driver`.

## 3. Key concepts learned along the way

### a) GitHub "page URL" vs "asset download URL"
- The release **page** lives at `.../releases/tag/<tag>` → serves HTML (for humans).
- The downloadable **file** lives at `.../releases/download/<tag>/<filename>` → the raw asset.
- Docker/`curl` must use the **`download`** form, or it grabs the web page instead of the jar.

### b) Dockerfile `ADD` needs **two** arguments
```dockerfile
ADD <source-url> <destination-path>
```
The whole URL must be on **one line**, followed by a space and the destination.

### c) `docker-compose` `build:` — short vs long form
- Short form `build: <dir>` expects a **context directory**, not a filename.
- To point at a specific Dockerfile, use the long form:
  ```yaml
  build:
    context: .                       # directory Docker can "see"
    dockerfile: metabase.dockerfile.yaml
  ```

### d) Named volumes must be **declared**
Using `metabase_data` in a service isn't enough — it must also be registered at
the bottom of the file:
```yaml
volumes:
  metabase_data:
```

### e) Two separate kinds of storage
- **`metabase_data` volume** → Metabase's *own* state (dashboards, users,
  settings), stored in an embedded **H2** database. **Not** DuckDB.
- **`./duckdb` mount** → *our* analytics data that Metabase **reads** via the
  DuckDB driver.
- Mental model: **H2 = Metabase's brain, DuckDB = the library it reads from.**

### f) Host path vs container path
Metabase runs *inside* the container, so it can't see host paths like
`/home/migiberto/...`. It only sees the **mount target**:
```
./duckdb   ->   /opt/dagster/duckdb     (host -> container)
```
So inside Metabase the database file path is `/opt/dagster/duckdb/data.duckdb`.

## 4. The big problem: Alpine (musl) vs the driver (glibc)

The official `metabase/metabase:latest` image is built on **Alpine Linux**,
which uses **musl libc**. The DuckDB driver ships a **native `.so` library
compiled against glibc**.

We hit two layers of errors:
1. `libstdc++.so.6: No such file` → tried `apk add libstdc++ libc6-compat`.
2. After that, deeper failures: `symbol not found` for `backtrace`,
   `backtrace_symbols`, `__res_init`, `__register_atfork`.

These are **glibc-only symbols** that musl (even with the `gcompat` shim) does
not provide. `libexecinfo` (which used to supply `backtrace`) was **removed from
modern Alpine**, so there is no clean `apk` fix. **Dead end.**

### How we diagnosed it
- Confirmed the file was readable inside the container and **not locked**.
- Opened `data.duckdb` with DuckDB 1.5.3 directly → healthy (3 tables). So the
  data was fine; the **driver load** was the problem.
- Ran `ldd` on the extracted `/tmp/libduckdb_java*.so` → it printed the exact
  missing glibc symbols.

## 5. The fix (recommended by the driver maintainers)

The `motherduckdb` maintainers' README says: don't fight Alpine — build Metabase
on a **Debian (glibc)** base. We rewrote `metabase.dockerfile.yaml` to:

1. Start from `eclipse-temurin:21-jre-jammy` (Debian + Java 21, **glibc**).
2. **Download `metabase.jar` ourselves** (v0.59.12 = "Metabase 59"), because we
   no longer inherit it from the official image.
3. Download the DuckDB driver (`1.5.3.0`, built for Metabase 59 + DuckDB 1.5.3)
   into the plugins folder.
4. Run as a non-root `metabase` user, set `MB_PLUGINS_DIR` and `MB_DB_FILE`,
   install `ca-certificates`, and launch with `java -jar`.

**Version pairing matters:** driver `1.5.3.0` is for **Metabase 59**, so we used
Metabase **v0.59.12** (not 60.x — also note Metabase can't *downgrade* its app
DB, which is why we wiped the stale `metabase_data` volume created by the older
`:latest` image).

## 6. Compose changes that came with the new image

- H2 volume remounted: `metabase_data:/metabase.db` → `metabase_data:/home/metabase/data`
  (must match `MB_DB_FILE=/home/metabase/data/metabase.db`).
- Removed the `MB_PLUGINS_DIR=/plugins` override (the Dockerfile sets it now).
- Kept `./duckdb:/opt/dagster/duckdb` and port `8080:3000`.

## 7. Build & run commands

```bash
# rebuild the image (downloads ~600 MB: metabase.jar + driver)
docker compose build metabase

# recreate the container from the new image
docker compose up -d --force-recreate metabase

# watch it boot (wait for "Metabase Initialization COMPLETE")
docker compose logs -f metabase

# quick health check
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/api/health   # -> 200
```

## 8. Connecting DuckDB in the Metabase UI

Open `http://localhost:8080`, finish the setup wizard, then add a database:
- **Type:** DuckDB
- **Database file:** `/opt/dagster/duckdb/data.duckdb`  (container path!)
- **Establish a read-only connection:** ON
- Leave **Motherduck Token** and **Azure transport** empty (watch for browser autofill).

## 9. Lessons for next time

- When a native driver says **"symbol not found"**, suspect a **glibc-vs-musl**
  mismatch (Alpine base) before anything else.
- **Check the project's README / reference Dockerfile early** — the maintainers
  already knew Alpine doesn't work; reading it first would have skipped the
  `apk` dead ends.
- **`ldd` on the `.so`** is the fastest way to see exactly which symbols/libs are missing.
- Match **driver version ↔ Metabase major version**, and remember Metabase
  **cannot downgrade** its app database.
