package io.pivotal.services.plugin.tasks.helper;

import io.pivotal.services.plugin.CfProperties;
import io.pivotal.services.plugin.CfRouteUtil;
import io.pivotal.services.plugin.ImmutableCfProperties;
import org.cloudfoundry.operations.CloudFoundryOperations;
import org.cloudfoundry.operations.applications.ApplicationDetail;
import org.cloudfoundry.operations.applications.ApplicationEnvironments;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.Optional;

import static org.cloudfoundry.util.tuple.TupleUtils.function;

/**
 * @author Biju Kunjummen
 * @author Gabriel Couto
 */
public class CfBlueGreenStage1Delegate {

    private CfPushDelegate pushDelegate = new CfPushDelegate();
    private CfAppDetailsDelegate appDetailsDelegate = new CfAppDetailsDelegate();
    private CfAppEnvDelegate appEnvDelegate = new CfAppEnvDelegate();

    private static final Logger LOGGER = Logging
        .getLogger(CfBlueGreenStage1Delegate.class);

    public Mono<Void> runStage1(Project project, CloudFoundryOperations cfOperations,
                                CfProperties cfProperties) {
        
        final String greenNameString = cfProperties.name() + "-green";
        final Mono<String> greenRouteStringMono = CfRouteUtil.getTempRoute(cfOperations, cfProperties, "green");

        Mono<Optional<ApplicationDetail>> appDetailMono = appDetailsDelegate
            .getAppDetails(cfOperations, cfProperties);

        // Get App Env Vars
        Mono<Optional<ApplicationEnvironments>> appEnvMono =
            appEnvDelegate.getAppEnv(cfOperations, cfProperties);

        Mono<ImmutableCfProperties> cfPropertiesMono = Mono.zip(appEnvMono, appDetailMono, greenRouteStringMono).map(function((appEnvOpt, appDetailOpt, greenRouteString) -> {
            LOGGER.lifecycle(
                "Running Blue Green Deploy - deploying a 'green' app. App '{}' with route '{}'",
                cfProperties.name(),
                greenRouteString);

            return appDetailOpt.map(appDetail -> {
                printAppDetail(appDetail);
                return ImmutableCfProperties.copyOf(cfProperties)
                    .withName(greenNameString)
                    .withHost(null)
                    .withDomain(null)
                    .withRoutes(Collections.singletonList(greenRouteString))
                    .withInstances(appDetail.getInstances())
                    .withMemory(appDetail.getMemoryLimit())
                    .withDiskQuota(appDetail.getDiskQuota());
            }).orElse(ImmutableCfProperties.copyOf(cfProperties)
                .withName(greenNameString)
                .withHost(null)
                .withDomain(null)
                .withRoutes(Collections.singletonList(greenRouteString)));
        }));

        return cfPropertiesMono.flatMap(
            withNewNameAndRoute -> pushDelegate.push(cfOperations, withNewNameAndRoute));
    }
    

    private void printAppDetail(ApplicationDetail applicationDetail) {
        LOGGER.lifecycle("Application Name: {}", applicationDetail.getName());
        LOGGER.lifecycle("Instance Count: {}", applicationDetail.getInstances());
        LOGGER
            .lifecycle("Running Instances: {}", applicationDetail.getRunningInstances());
    }
}
