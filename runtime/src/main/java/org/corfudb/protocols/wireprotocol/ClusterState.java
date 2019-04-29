package org.corfudb.protocols.wireprotocol;

import com.google.common.collect.ImmutableMap;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.Singular;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity;
import org.corfudb.protocols.wireprotocol.failuredetector.NodeConnectivity.NodeConnectivityType;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Records the cluster state of the system.
 * This includes a map of {@link NodeState}.
 *
 * <p>Created by zlokhandwala on 11/1/18.
 */
@Data
@Builder
@AllArgsConstructor
@ToString
@Slf4j
public class ClusterState implements ICorfuPayload<ClusterState> {

    /**
     * Node's view of the cluster. The node collects states from all the other nodes in the cluster.
     * For instance, three node cluster:
     * {"a": {"endpoint": "a", "connectivity":{"a": true, "b": true, "c": true}}}
     * {"b": {"endpoint": "b", "connectivity":{"a": true, "b": true, "c": false}}}
     * {"c": {"endpoint": "c", "connectivity":{"a": true, "b": false, "c": true}}}
     */
    @Singular
    private final ImmutableMap<String, NodeState> nodes;

    @NonNull
    private final String localEndpoint;

    public ClusterState(ByteBuf buf) {
        nodes = ImmutableMap.copyOf(ICorfuPayload.mapFromBuffer(buf, String.class, NodeState.class));
        localEndpoint = ICorfuPayload.fromBuffer(buf, String.class);
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        ICorfuPayload.serialize(buf, nodes);
        ICorfuPayload.serialize(buf, localEndpoint);
    }

    public int size() {
        return nodes.size();
    }

    public Optional<NodeState> getNode(String endpoint) {
        return Optional.ofNullable(nodes.get(endpoint));
    }

    /**
     * See if cluster is ready. If cluster contains at least one node with state NOT_READY the cluster is not ready and
     * cluster state can't be used to find failures.
     *
     * @return cluster status
     */
    public boolean isReady() {
        if (nodes.isEmpty()) {
            log.error("Invalid ClusterState: is empty");
            return false;
        }

        if (!checkEpochs()) {
            log.info("ClusterState is not consistent: {}", nodes);
            return false;
        }

        //if at least one node is not ready then entire cluster is not ready to provide correct information
        for (NodeState nodeState : nodes.values()) {
            if (nodeState.getConnectivity().getType() == NodeConnectivityType.NOT_READY) {
                return false;
            }
        }

        return true;
    }

    private boolean checkEpochs() {
        long currentEpoch =  -1;

        for (NodeState nodeState : nodes.values()) {
            NodeConnectivity connectivity = nodeState.getConnectivity();
            if (currentEpoch == -1) {
                currentEpoch = connectivity.getEpoch();
                continue;
            }

            if (connectivity.getEpoch() != currentEpoch) {
                return false;
            }
        }
        return true;
    }

    public NodeConnectivity getLocalNodeConnectivity() {
        NodeState nodeState = getNode(localEndpoint)
                .orElseThrow(() -> new IllegalArgumentException("Node not found: " + localEndpoint));

        return nodeState.getConnectivity();
    }

    public static ClusterState buildClusterState(String localEndpoint, NodeState... states) {
        Map<String, NodeState> graph = Arrays.stream(states)
                .collect(Collectors.toMap(state -> state.getConnectivity().getEndpoint(), Function.identity()));

        return ClusterState.builder()
                .localEndpoint(localEndpoint)
                .nodes(ImmutableMap.copyOf(graph))
                .build();
    }
}
