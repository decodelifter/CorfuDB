package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import org.assertj.core.data.MapEntry;
import org.corfudb.runtime.collections.CorfuQueue.CorfuQueueRecord;
import org.corfudb.runtime.view.AbstractViewTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class CorfuQueueTest extends AbstractViewTest {

    Collection<String> project(Collection<Map.Entry<String, String>> entries) {
        return entries.stream().map(entry -> entry.getValue()).collect(Collectors.toCollection(ArrayList::new));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void basicQueueOrder() {
        CorfuQueue<String>
                corfuQueue = getDefaultRuntime().getObjectsView().build()
                .setTypeToken(new TypeToken<CorfuQueue<String>>() {})
                .setStreamName("test")
                .open();

        long idC = corfuQueue.enqueue("c");
        long idB = corfuQueue.enqueue("b");
        long idA = corfuQueue.enqueue("a");


        List<CorfuQueueRecord<String>> records = corfuQueue.entryList(-1);

        assertThat(records).hasSize(3);
        assertThat(records.get(0).getID()).isEqualTo(idC);
        assertThat(records.get(1).getID()).isEqualTo(idB);
        assertThat(records.get(2).getID()).isEqualTo(idA);

        assertThat(records.get(0).getEntry()).isEqualTo("c");
        assertThat(records.get(1).getEntry()).isEqualTo("b");
        assertThat(records.get(2).getEntry()).isEqualTo("a");

        // Remove the middle entry
        corfuQueue.remove(idB);

        List<CorfuQueueRecord<String>> records2 = corfuQueue.entryList(-1);
        assertThat(records2.get(0).getEntry()).isEqualTo("c");
        assertThat(records2.get(1).getEntry()).isEqualTo("a");
    }
}
