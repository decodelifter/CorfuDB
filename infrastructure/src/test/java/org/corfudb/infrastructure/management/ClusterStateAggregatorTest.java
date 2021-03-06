package org.corfudb.infrastructure.management;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.management.NodeStateTestUtil.nodeState;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.FAILED;
import static org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.ConnectionStatus.OK;

import org.corfudb.protocols.wireprotocol.ClusterState;
import org.corfudb.protocols.wireprotocol.NodeState;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class ClusterStateAggregatorTest {

    @Test
    public void getAggregatedStateSingleNodeCluster() {
        final String localEndpoint = "a";

        ClusterState clusterState = ClusterState.buildClusterState(
                localEndpoint,
                nodeState("a", OK)
        );

        List<ClusterState> clusterStates = Arrays.asList(clusterState, clusterState, clusterState);
        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(localEndpoint)
                .clusterStates(clusterStates)
                .build();

        NodeState expectedNodeState = clusterState.getNode("a").get();
        NodeState actualNodeState = aggregator.getAggregatedState().getNode("a").get();
        assertThat(actualNodeState).isEqualTo(expectedNodeState);
    }

    @Test
    public void getAggregatedState() {
        final String localEndpoint = "a";

        ClusterState clusterState1 = ClusterState.buildClusterState(
                localEndpoint,
                nodeState("a", OK, FAILED, FAILED),
                NodeState.getUnavailableNodeState("b"),
                NodeState.getUnavailableNodeState("c")
        );

        ClusterState clusterState2 = ClusterState.buildClusterState(
                localEndpoint,
                nodeState("a", OK, OK, FAILED),
                nodeState("b", OK, OK, FAILED),
                NodeState.getUnavailableNodeState("c")
        );

        final int epoch = 1;
        final int counter = 123;
        ClusterState clusterState3 = ClusterState.buildClusterState(
                localEndpoint,
                nodeState("a", OK, FAILED, FAILED),
                NodeState.getUnavailableNodeState("b"),
                NodeState.getNotReadyNodeState("c", epoch, counter)
        );

        List<ClusterState> clusterStates = Arrays.asList(
                clusterState1, clusterState2, clusterState3
        );

        ClusterStateAggregator aggregator = ClusterStateAggregator.builder()
                .localEndpoint(localEndpoint)
                .clusterStates(clusterStates)
                .build();

        //check [CONNECTED, CONNECTED, CONNECTED]
        NodeState expectedLocalNodeState = clusterState3.getNode(localEndpoint).get();
        NodeState localNodeState = aggregator.getAggregatedState().getNode(localEndpoint).get();
        assertThat(localNodeState.isConnected()).isTrue();
        assertThat(localNodeState).isEqualTo(expectedLocalNodeState);

        //check [UNAVAILABLE, CONNECTED, UNAVAILABLE]
        NodeState expectedNodeBState = clusterState2.getNode("b").get();
        NodeState nodeBState = aggregator.getAggregatedState().getNode("b").get();
        assertThat(nodeBState.isConnected()).isTrue();
        assertThat(nodeBState).isEqualTo(expectedNodeBState);

        //check [UNAVAILABLE, UNAVAILABLE, NOT_READY]
        NodeState expectedNodeCState = clusterState3.getNode("c").get();
        NodeState nodeCState = aggregator.getAggregatedState().getNode("c").get();
        assertThat(nodeCState.isConnected()).isFalse();
        assertThat(nodeCState).isEqualTo(expectedNodeCState);
    }
}
