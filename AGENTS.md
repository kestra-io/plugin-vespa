# Kestra Vespa Plugin

## What

- Provides plugin components under `io.kestra.plugin.vespa`.
- Includes the `Query` task, which runs one YQL query on the Vespa query API and returns the hits as rows, or writes them to an ION file in Kestra's internal storage with `fetchType: STORE`.
- Includes the `Get` task, which reads one document by its ID from the Vespa document API and returns its fields, or a null `document` when it doesn't exist (`errorOnMissing: true` fails instead).
- Includes the `Delete` task, which removes one document by its ID, or every document a selection expression matches in a content cluster with `allowSelection: true`, and returns the `deleted` count for a selection.
- The connection (`endpoint`, `token`, timeouts) lives in `VespaConnectionInterface` and `AbstractVespaTask`, shared by the tasks.

## Why

- What user problem does this solve? Teams query Vespa from flows with `curl` in shell tasks, with no typed outputs and no secret masking for the data-plane token.
- Why would a team adopt this plugin in a workflow? It runs Vespa queries as a typed task in the same Kestra flow as the steps that feed and use the results, with retries, secrets and notifications.
- What operational/business outcome does it enable? Search and retrieval steps (text, vector and hybrid queries) become part of orchestrated, observable pipelines instead of ad hoc scripts.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `vespa`

Infrastructure dependencies (Docker Compose services):

- `app`

Tests run Vespa itself in a Testcontainers container (`VespaContainer`, image `vespaengine/vespa`), deployed with the
application package and catalog under `src/test/resources/vespa/`.

### Key Plugin Classes

- `io.kestra.plugin.vespa.Query`
- `io.kestra.plugin.vespa.Get`
- `io.kestra.plugin.vespa.Delete`
- `io.kestra.plugin.vespa.AbstractVespaTask`
- `io.kestra.plugin.vespa.VespaConnectionInterface`

### Project Structure

```
plugin-vespa/
├── src/main/java/io/kestra/plugin/vespa/
├── src/test/java/io/kestra/plugin/vespa/
├── build.gradle
└── README.md
```

## Local rules

- Base the wording on the implemented packages and classes, not on template README text.

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
