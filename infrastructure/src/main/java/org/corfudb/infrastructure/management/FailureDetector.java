package org.corfudb.infrastructure.management;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.infrastructure.RemoteMonitoringService;
import org.corfudb.infrastructure.management.ClusterStateContext.HeartbeatCounter;
import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.corfudb.protocols.wireprotocol.SequencerMetrics;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.view.Layout;
import org.corfudb.util.CFUtils;
import org.corfudb.util.Sleep;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * FailureDetector polls all the "responsive members" in the layout.
 * Responsive members: All endpoints that are responding to heartbeats. This list can be derived by
 * excluding unresponsiveServers from all endpoints.
 * For every poll method call invoked, it starts a polling round and generates a poll report.
 * Each polling round comprises of "failureThreshold" number of iterations.
 * - We asynchronously poll every known responsive member in the layout.
 * - Poll result aggregation.
 * - If we complete an iteration without detecting failures, we end the round successfully.
 * The management Server ensures only one instance of this class and hence this is NOT thread safe.
 * Created by zlokhandwala on 11/29/17.
 */
@Slf4j
public class FailureDetector implements IDetector {


    /**
     * Number of iterations to execute to detect a failure in a round.
     */
    @Getter
    @Setter
    private int failureThreshold = 3;

    /**
     * Max duration for the responseTimeouts of the routers in milliseconds.
     * In the worst case scenario or in case of failed servers, their response timeouts will be
     * set to a maximum value of maxPeriodDuration.
     */
    @Getter
    @Setter
    private long maxPeriodDuration = 5_000L;

    /**
     * Minimum duration for the responseTimeouts of the routers in milliseconds.
     * Under ideal conditions the routers will have a response timeout set to this.
     */
    @Getter
    @Setter
    private long initPeriodDuration = 2_000L;

    /**
     * Response timeout for every router.
     */
    @Getter
    private long period = initPeriodDuration;

    /**
     * Poll interval between iterations in a pollRound in milliseconds.
     */
    @Getter
    @Setter
    private long initialPollInterval = 1_000;

    /**
     * Increments in which the period moves towards the maxPeriodDuration in every failed
     * iteration provided in milliseconds.
     */
    @Getter
    @Setter
    private long periodDelta = 1_000L;

    @NonNull
    private final HeartbeatCounter heartbeatCounter;

    @NonNull
    private final String localEndpoint;

    public FailureDetector(HeartbeatCounter heartbeatCounter, String localEndpoint) {
        this.heartbeatCounter = heartbeatCounter;
        this.localEndpoint = localEndpoint;
    }

    /**
     * Executes the policy once.
     * Checks for changes in the layout.
     * Then polls all the servers generates pollReport.
     *
     * @param layout Current Layout
     */
    public PollReport poll(
            @Nonnull Layout layout, @Nonnull CorfuRuntime corfuRuntime, @NonNull SequencerMetrics sequencerMetrics) {

        log.trace("Poll report. Layout: {}", layout);

        Map<String, IClientRouter> routerMap;

        // Collect and set all responsive servers in the members array.
        Set<String> allServers = layout.getAllServers();

        // Set up arrays for routers to the endpoints.
        routerMap = new HashMap<>();
        allServers.forEach(s -> {
            IClientRouter router = corfuRuntime.getRouter(s);
            router.setTimeoutResponse(period);
            routerMap.put(s, router);
        });
        // Perform polling of all responsive servers.
        return pollRound(layout.getEpoch(), allServers, routerMap, sequencerMetrics, layout);
    }

    /**
     * PollRound consists of iterations. In each iteration, the FailureDetector pings all the
     * responsive nodes in the layout and also collects their {@link NodeState}-s to provide {@link ClusterState}.
     * Failure detector collect a number of {@link PollReport}-s equals to failureThreshold number then
     * aggregates final {@link PollReport}.
     * Aggregation step:
     * - go through all reports
     * - aggregate all wrong epochs from all intermediate poll reports then remove all responsive nodes from the list.
     * If wrongEpochsAggregated variable still contains wrong epochs
     * then it will be corrected by {@link RemoteMonitoringService}
     * - Aggregate connected and failed nodes from all reports
     * - Timeouts tuning
     * - provide a poll report based on latest pollIteration report and aggregated values
     *
     * @return Poll Report with detected failed nodes and out of phase epoch nodes.
     */
    private PollReport pollRound(
            long epoch, Set<String> allServers, Map<String, IClientRouter> router,
            SequencerMetrics sequencerMetrics, Layout layout) {

        if (failureThreshold < 1) {
            throw new IllegalStateException("Invalid failure threshold");
        }

        List<PollReport> reports = new ArrayList<>();
        for (int iteration = 0; iteration < failureThreshold; iteration++) {
            long pollIterationStartTime = System.nanoTime();

            PollReport currReport = pollIteration(allServers, router, epoch, sequencerMetrics, layout);
            reports.add(currReport);

            long pollInterval = modifyIterationTimeouts(router, currReport, pollIterationStartTime);

            // Sleep for the provided poll interval before starting the next iteration
            Sleep.MILLISECONDS.sleepUninterruptibly(pollInterval);
        }

        //Aggregation step
        Map<String, Long> wrongEpochsAggregated = new HashMap<>();
        Set<String> connectedNodesAggregated = new HashSet<>();
        Set<String> failedNodesAggregated = new HashSet<>();

        reports.forEach(report -> {
            //Calculate wrong epochs
            wrongEpochsAggregated.putAll(report.getWrongEpochs());
            report.getReachableNodes().forEach(wrongEpochsAggregated::remove);

            //Aggregate failed/connected nodes
            connectedNodesAggregated.addAll(report.getReachableNodes());
            failedNodesAggregated.addAll(report.getFailedNodes());
        });
        failedNodesAggregated.removeAll(connectedNodesAggregated);

        Set<String> allConnectedNodes = Sets.union(connectedNodesAggregated, wrongEpochsAggregated.keySet());

        tunePollReportTimeouts(router, failedNodesAggregated, allConnectedNodes);

        List<ClusterState> clusterStates = reports.stream()
                .map(PollReport::getClusterState)
                .collect(Collectors.toList());

        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(localEndpoint)
                .clusterStates(clusterStates)
                .build();

        return PollReport.builder()
                .pollEpoch(epoch)
                .responsiveServers(layout.getActiveLayoutServers())
                .wrongEpochs(ImmutableMap.copyOf(wrongEpochsAggregated))
                .clusterState(aggregator.getAggregatedState())
                .build();
    }

    /**
     * Tune timeouts after each poll iteration, according to list of failed and connected nodes
     *
     * @param router     client router
     * @param currReport current poll report
     * @return updated poll interval
     */
    private long modifyIterationTimeouts(Map<String, IClientRouter> router, PollReport currReport,
                                         long pollIterationStartTime) {
        long pollInterval = initialPollInterval;

        Set<String> allReachableNodes = currReport.getAllReachableNodes();

        if (!currReport.getFailedNodes().isEmpty()) {
            // Increase the inter iteration sleep time by increasing poll interval if the
            // period time increases
            long iterationElapsedTime = TimeUnit.MILLISECONDS.convert(
                    System.nanoTime() - pollIterationStartTime,
                    TimeUnit.NANOSECONDS
            );
            pollInterval = Math.max(initialPollInterval, period - iterationElapsedTime);
            period = getIncreasedPeriod();
            tuneRoutersResponseTimeout(router, allReachableNodes, period);
        }
        return pollInterval;
    }

    /**
     * We can try to scale back the network latency time after every poll round.
     * If there are no failures, after a few rounds our polling period converges back to initPeriodDuration.
     *
     * @param clientRouters     clientRouters
     * @param failedNodes       list of failed nodes
     * @param allConnectedNodes all connected nodes
     */
    private void tunePollReportTimeouts(
            Map<String, IClientRouter> clientRouters, Set<String> failedNodes, Set<String> allConnectedNodes) {
        period = Math.max(initPeriodDuration, period - periodDelta);
        tuneRoutersResponseTimeout(clientRouters, allConnectedNodes, period);
        // Reset the timeout of all the failed nodes to the max value to set a longer
        // timeout period to detect their response.
        tuneRoutersResponseTimeout(clientRouters, failedNodes, maxPeriodDuration);
    }

    /**
     * Poll iteration step, provides a {@link PollReport} composed from pings and {@link NodeState}-s collected by
     * this node from the cluster.
     * Algorithm:
     * - ping all nodes
     * - collect all node states
     * - collect wrong epochs
     * - collect connected/failed nodes
     * - calculate if current layout slot is unfilled
     * - build poll report
     *
     * @param allServers       all servers in the cluster
     * @param clientRouters    client clientRouters
     * @param epoch            current epoch
     * @param sequencerMetrics metrics
     * @param layout           current layout
     * @return a poll report
     */
    private PollReport pollIteration(
            Set<String> allServers, Map<String, IClientRouter> clientRouters, long epoch,
            SequencerMetrics sequencerMetrics, Layout layout) {

        log.trace("Poll iteration. Epoch: {}", epoch);

        ClusterStateCollector clusterCollector = ClusterStateCollector.builder()
                .localEndpoint(localEndpoint)
                .clusterState(pollAsync(allServers, clientRouters, epoch))
                .heartbeatCounter(heartbeatCounter)
                .build();

        //Cluster state internal map.
        ClusterState clusterState = clusterCollector.collectClusterState(epoch, sequencerMetrics);

        return PollReport.builder()
                .pollEpoch(epoch)
                .responsiveServers(layout.getActiveLayoutServers())
                .wrongEpochs(clusterCollector.collectWrongEpochs())
                .clusterState(clusterState)
                .build();
    }

    /**
     * Poll all members servers once asynchronously and store their futures in
     * pollCompletableFutures.
     *
     * @param allServers    All active members in the layout.
     * @param clientRouters Map of routers for all active members.
     * @param epoch         Current epoch for the polling round to stamp the ping messages.
     * @return Map of Completable futures for the pings.
     */
    private Map<String, CompletableFuture<NodeState>> pollAsync(
            Set<String> allServers, Map<String, IClientRouter> clientRouters, long epoch) {
        // Poll servers for health.  All ping activity will happen in the background.
        Map<String, CompletableFuture<NodeState>> clusterState = new HashMap<>();
        allServers.forEach(s -> {
            try {
                clusterState.put(s, new ManagementClient(clientRouters.get(s), epoch).sendNodeStateRequest());
            } catch (Exception e) {
                CompletableFuture<NodeState> cf = new CompletableFuture<>();
                cf.completeExceptionally(e);
                clusterState.put(s, cf);
            }
        });

        //Ping all nodes in parallel.
        //Possible exceptions are held by their CompletableFutures. They will be handled in pollIteration method
        try {
            CFUtils.allOf(clusterState.values()).join();
        } catch (Exception ex) {
            //ignore
        }

        return clusterState;
    }

    /**
     * Function to increment the existing response timeout period.
     *
     * @return The new calculated timeout value.
     */
    private long getIncreasedPeriod() {
        return Math.min(maxPeriodDuration, period + periodDelta);
    }

    /**
     * Set the timeoutResponse for all the routers connected to the given endpoints with the
     * given value.
     *
     * @param endpoints Router endpoints.
     * @param timeout   New timeout value.
     */
    private void tuneRoutersResponseTimeout(Map<String, IClientRouter> clientRouters, Set<String> endpoints, long timeout) {

        log.trace("Tuning router timeout responses for endpoints:{} to {}ms", endpoints, timeout);
        endpoints.forEach(server -> {
            if (clientRouters.get(server) != null) {
                clientRouters.get(server).setTimeoutResponse(timeout);
            }
        });
    }

}
