package io.kestra.plugin.vespa;

import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.Task;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class AbstractVespaTask extends Task implements VespaConnectionInterface {
    private Property<String> endpoint;

    @ToString.Exclude
    private Property<String> token;

    private Property<Duration> connectTimeout;

    private Property<Duration> readTimeout;
}
