package io.kestra.plugin.vespa;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.junit.annotations.LoadFlows;
import io.kestra.core.models.flows.State;
import io.kestra.core.runners.TestRunnerUtils;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.kestra.core.tenant.TenantService.MAIN_TENANT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@KestraTest(startRunner = true)
class RunnerTest extends VespaContainer {
    @Inject
    private TestRunnerUtils runnerUtils;

    // @ExecuteFlow can't pass inputs, and the flow needs the harness's random port
    @Test
    @LoadFlows("sanity-checks/query.yaml")
    void query() throws Exception {
        var execution = runnerUtils.runOne(MAIN_TENANT, "sanitychecks.plugin-vespa", "query", null, (flow, e) -> Map.of("endpoint", endpoint()));

        assertThat(execution.getTaskRunList(), hasSize(4));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @LoadFlows("sanity-checks/get.yaml")
    void get() throws Exception {
        var execution = runnerUtils.runOne(MAIN_TENANT, "sanitychecks.plugin-vespa", "get", null, (flow, e) -> Map.of("endpoint", endpoint()));

        assertThat(execution.getTaskRunList(), hasSize(4));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @LoadFlows("sanity-checks/delete.yaml")
    void delete() throws Exception {
        var execution = runnerUtils.runOne(MAIN_TENANT, "sanitychecks.plugin-vespa", "delete", null, (flow, e) -> Map.of("endpoint", endpoint()));

        assertThat(execution.getTaskRunList(), hasSize(11));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @LoadFlows("sanity-checks/feed.yaml")
    void feed() throws Exception {
        var execution = runnerUtils.runOne(MAIN_TENANT, "sanitychecks.plugin-vespa", "feed", null, (flow, e) -> Map.of("endpoint", endpoint()));

        assertThat(execution.getTaskRunList(), hasSize(5));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }

    @Test
    @LoadFlows("sanity-checks/feed-rendering.yaml")
    void feedRendering() throws Exception {
        var execution = runnerUtils.runOne(MAIN_TENANT, "sanitychecks.plugin-vespa", "feed-rendering", null, (flow, e) -> Map.of("endpoint", endpoint()));

        assertThat(execution.getTaskRunList(), hasSize(5));
        assertThat(execution.getState().getCurrent(), is(State.Type.SUCCESS));
    }
}
