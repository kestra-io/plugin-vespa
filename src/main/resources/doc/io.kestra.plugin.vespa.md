# How to use the Vespa plugin

Query a [Vespa](https://docs.vespa.ai/) application from Kestra flows with YQL (text, vector and hybrid search), and
read documents by their ID.

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
`{{ outputs.search.rows[0].fields.album }}`. `fetchType` picks `FETCH` (rows, default), `FETCH_ONE` (first row),
`STORE` (the rows in an ION file in Kestra's internal storage, returned as `uri`, for the next task to read) or `NONE`
(only `totalCount`). Vespa returns 10 hits by default and refuses more than 400 hits or an offset over 1000
unless the application raises its limits.

`Get` reads one document by its full ID (`documentId`, like `id:mynamespace:music::kind-of-blue`, or with a
`n=<number>` or `g=<group>` part) from Vespa's [document API](https://docs.vespa.ai/en/reference/document-v1-api-reference.html)
and returns `id` and `document`, the document's fields. A `Query` hit's `id` can be passed straight in when the hit carries the document ID, as with `select *`. A document
that doesn't exist gives a null `document`; set `errorOnMissing: true` to fail the task instead.
