# How to use the Vespa plugin

Query a [Vespa](https://docs.vespa.ai/) application from Kestra flows with YQL (text, vector and hybrid search), read
documents by their ID, delete documents by ID or by selection, feed many documents at once, and start a flow when new
documents match a query.

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
and returns `id` and `document`, the document's fields. A `Query` hit's `id` can be passed straight in when the hit
carries the document ID, as with `select *`. A document that doesn't exist gives a null `document`; set
`errorOnMissing: true` to fail the task instead.

`Delete` removes one document by its full ID (`documentId`, same format as `Get`), or every document a
[document selection](https://docs.vespa.ai/en/reference/document-select-language.html) matches (`selection`, like
`music.artist == "Miles Davis"`) in the content cluster `cluster`. Set exactly one of `documentId` and `selection`. A
delete by selection fails before sending anything unless `allowSelection: true`, since `selection: true` matches every
document, except those of a global document type (`global="true"`), which a selection doesn't reach. Vespa deletes in
chunks of up to 60 seconds each; with `readTimeout` set, each chunk is kept to half of it. Once Vespa has deleted
documents, a failure names how many were already removed. It returns `deleted`, the number of documents removed; for
a delete by ID Vespa answers the same whether the document existed, so `deleted` is null and a mistyped ID succeeds
without removing anything.

`Feed` writes (`PUT`, default), updates (`UPDATE`, with `fields` in Vespa's
[update format](https://docs.vespa.ai/en/reference/document-json-format.html#update)) or removes (`REMOVE`) many
documents through Vespa's [feed client](https://docs.vespa.ai/en/vespa-feed-client.html), which retries throttled
answers. `from` takes the records inline or as a `kestra://` file from a previous task (Kestra reads that file
whole into memory before the first record is sent, so split very large files); each record is a map with
`id` and `fields` (`REMOVE` needs only `id`). An `id` that starts with `id:` is the full document ID; any other `id` is
a key, completed with `namespace` and `documentType`. It returns `successCount` and `failureCount`. A record Vespa
refuses fails the task once the others are written; with `failOnError: false` the task ends in `WARNING` and `uri`
holds the failed records, each row with the `record` as given and its `error`. Each operation gets `readTimeout`
(5 minutes when unset); one that runs out may still have been applied by Vespa. `connectTimeout` doesn't apply to
`Feed`, and its `endpoint` must have no path (only scheme, host and port): the feed client always sends to the
host's root. Before feeding, `Feed` sends one `HEAD /state/v1/health` through Kestra's HTTP client, so
Kestra's allowed and denied hosts (`kestra.tasks.http.allowed-list` and `denied-list`, from Kestra 2.0) apply to it
too.

`createIfNotExists: true` turns an `UPDATE` of a missing document into an upsert. `condition` is Vespa's
[test-and-set](https://docs.vespa.ai/en/writing/document-v1-api-guide.html#conditional-writes) condition, like
`music.year==2000`: a record whose document doesn't match, or doesn't exist, is a failed record. With both, a missing
document is written without checking the condition; Vespa also writes it when every replica of an existing document
is unreachable ([Vespa's warning](https://docs.vespa.ai/en/writing/document-v1-api-guide.html#conditional-updates-and-puts-with-create)).
`maxInFlight` (1 to 4096, default 4096) caps the records waiting for Vespa's answer, to put less load on Vespa.

## Triggers

`Trigger` runs a YQL query (`yql`) every `interval` (default 60 seconds) and starts the flow with the documents it
hasn't delivered before, as `trigger.rows` in the same shape as `Query`, or in an ION file at `trigger.uri` with
`fetchType: STORE`; `trigger.size` is how many were delivered. `watermarkField` names a whole-number attribute that
grows with every write, like the time the document was fed: the trigger keeps the highest value it delivered, and
the document IDs at that value, in the namespace KV store, and asks Vespa only for documents at or above it, oldest
first, through Vespa's `recall` and `sorting` parameters, so the YQL is sent as written. A document written later
with a lower value is never delivered, and a removed document fires nothing. The first poll delivers what already
matches, up to `hits` (default 100) per poll, so a large backlog arrives over several polls. Each hit must carry its
document ID and the `watermarkField` value, as `select *` does; the YQL can't have an `order by`, an `offset` or a
`limit`. Keep the field fine-grained (like milliseconds): once more documents share its highest value than Vespa's
`maxHits` (400 by default) leaves room for, the poll fails with a message to raise it. A document without a
`watermarkField` value is never delivered. If Vespa searched only part of the corpus, the poll delivers nothing and the
next one tries again. On Kestra 2.0 with `allowConcurrent: true` a slow poll can overlap the next one and deliver the
same documents twice.
