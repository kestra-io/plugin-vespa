# How to use the Vespa plugin

Query a [Vespa](https://docs.vespa.ai/) application from Kestra flows with YQL: text, vector and hybrid search.

## Authentication

Set `endpoint` to the base URL of the Vespa container cluster, like `http://localhost:8080` or a Vespa Cloud endpoint.
For Vespa Cloud, set `token` to a data-plane token, sent as a bearer token; leave it unset for a self-hosted Vespa
without authentication. Store the token in a [secret](https://kestra.io/docs/concepts/secret), and set the connection
once for every task with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`Query` runs one YQL query (`yql`) on Vespa's [query API](https://docs.vespa.ai/en/reference/query-api-reference.html)
and returns the hits of that one answer. Optional: `query` (text for `userQuery()`), `hits`, `offset`, `ranking` (rank
profile), `queryTimeout` (Vespa's own time limit) and `parameters` for any other query API parameter, like a query
tensor `input.query(q)`. A parameter that a task property also sets fails the task, so each value has one place.

Rows keep Vespa's hit shape: `id`, `relevance` and the document under `fields`, read like
`{{ outputs.search.rows[0].fields.album }}`. `fetchType` picks `FETCH` (rows, default), `FETCH_ONE` (first row) or
`NONE` (only `totalCount`). Vespa returns 10 hits by default and refuses more than 400 hits or an offset over 1000
unless the application raises its limits.
