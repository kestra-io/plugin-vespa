@PluginSubGroup(
    title = "Vespa",
    description = "Tasks that query Vespa, the search and vector engine, with YQL for text, vector and hybrid search, read documents by their ID, delete documents by ID or by selection, and feed documents in bulk.",
    categories = {
        PluginSubGroup.PluginCategory.DATA,
        PluginSubGroup.PluginCategory.AI
    }
)
package io.kestra.plugin.vespa;

import io.kestra.core.models.annotations.PluginSubGroup;