package org.corfudb.runtime.collections;

import com.google.common.reflect.TypeToken;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.annotations.Accessor;
import org.corfudb.annotations.ConflictParameter;
import org.corfudb.annotations.CorfuObject;
import org.corfudb.annotations.DontInstrument;
import org.corfudb.annotations.Mutator;
import org.corfudb.annotations.MutatorAccessor;

import java.util.*;

/**
 * Persisted Queue supported by CorfuDB using distributed State Machine Replication.
 * Entries enqueued are backed by a LinkedHashMap where each entry is mapped to a unique generated
 * id that is returned upon successful <b>enqueue()</b>.
 * Consumption is via the <b>entryList()</b> which returns entries along with their ids sorted by
 * the order in which their <b>enqueue()</b> operations materialized. Thus a logical FIFO queue.
 * Instead of a dequeue() this Queue supports a <b>remove()</b> which accepts the id of the element.
 * Entries cannot be modified in-place and but can be removed from anywhere.
 *
 * Created by Sundar Sridharan on 5/8/19.
 *
 * @param <E>   Type of the entry to be enqueued into the persisted queue
 */
@Slf4j
@CorfuObject
public class CorfuQueue<E> implements ISMRObject {
    /** The "main" linked map which contains the primary key-value mappings. */
    private final Map<Long, E> mainMap = new LinkedHashMap<>();

    /** Returns the size of the queue at a given point in time. */
    @Accessor
    public int size() {
        return mainMap.size();
    }

    /**
     * Appends the specified element at the end of this unbounded queue.
     * In a distributed system, the order of insertions cannot be guaranteed
     * unless a transaction is used.
     * Capacity restrictions and backoffs must be implemented outside this
     * interface. Consider validating the size of the queue against a high
     * watermark before enqueue.
     *
     * @param e the element to add
     * @throws NullPointerException if the specified element is null and this
     *         queue does not permit null elements
     * @throws IllegalArgumentException if some property of the specified
     *         element prevents it from being added to this queue
     * @return EntryId representing this object in the persistent queue
     */
    public long enqueue(E e) {
        long id = new Random().nextLong();
        put(id, e);
        return id;
    }

    /** CorfuQueueRecord encapsulates each entry enqueued into CorfuQueue with its unique ID.
     * It is a read-only type returned by the entryList() method.
     * The ID returned here can be used for both point get()s as well as remove() operations
     * on this Queue.
     *
     * @param <E>
     */
    public static class CorfuQueueRecord<E> {
        @Getter
        final Long ID;

        @Getter
        final E entry;

        CorfuQueueRecord(long id, E entry) {
            this.ID = id;
            this.entry = entry;
        }
    }

    /**
     * Returns a List of CorfuQueueRecords sorted by the order in which the enqueue materialized.
     *
     * {@inheritDoc}
     *
     * <p>This function currently does not return a view like the java.util implementation,
     * and changes to the entryList will *not* be reflected in the map. </p>
     *
     * @param maxEntires - Limit the number of entries returned from start of the queue
     *                   -1 implies all entries
     * @return List of Entries sorted by their enqueue order
     */
    public List<CorfuQueueRecord<E>> entryList(int maxEntires) {
        if (maxEntires == -1) {
            maxEntires = Integer.MAX_VALUE;
        }
        List<CorfuQueueRecord<E>> copy = new ArrayList<>(Math.min(mainMap.size(), maxEntires));
        for (Map.Entry<Long, E> entry : mainMap.entrySet()) {
            copy.add(new CorfuQueueRecord<>(entry.getKey(),
                    entry.getValue()));
            if (--maxEntires == 0) {
                break;
            }
        }
        return copy;
    }

    @Accessor
    public boolean isEmpty() {
        return mainMap.isEmpty();
    }

    /** {@inheritDoc} */
    @Accessor
    public boolean containsKey(@ConflictParameter Long key) {
        return mainMap.containsKey(key);
    }

    /** {@inheritDoc} */
    @Accessor
    public E get(@ConflictParameter Long key) {
        return mainMap.get(key);
    }

    /**
     * Removes a specific element identified by the ID returned via entryList()'s CorfuQueueRecord.
     *
     * {@inheritDoc}
     *
     * @throws NoSuchElementException if this queue did not contain this element
     * @return The entry that was successfully removed
     */
    @MutatorAccessor(name = "remove", undoFunction = "undoRemove",
                                undoRecordFunction = "undoRemoveRecord")
    @SuppressWarnings("unchecked")
    public E remove(@ConflictParameter Long key) {
        E previous =  mainMap.remove(key);
        return previous;
    }

    /**
     * Erase all entries from the Queue.
     */
    @Mutator(name = "clear", reset = true)
    public void clear() {
        mainMap.clear();
    }

    /** Helper function to get a Corfu Queue.
     *
     * @param <E>                   Queue entry type
     * @return                      A type token to pass to the builder.
     */
    static <E>
    TypeToken<CorfuQueue<E>>
        getQueueType() {
            return new TypeToken<CorfuQueue<E>>() {};
    }

    /** {@inheritDoc} */
    @MutatorAccessor(name = "put", undoFunction = "undoPut", undoRecordFunction = "undoPutRecord")
    protected E put(@ConflictParameter long key, E value) {
        E previous = mainMap.put(key, value);
        return previous;
    }

    @DontInstrument
    protected E undoPutRecord(CorfuQueue<E> queue, long key, E value) {
        return queue.mainMap.get(key);
    }

    @DontInstrument
    protected void undoPut(CorfuQueue<E> queue, E undoRecord, long key, E entry) {
        // Same as undoRemove (restore previous value)
        undoRemove(queue, undoRecord, key);
    }

    enum UndoNullable {
        NULL;
    }

    @DontInstrument
    protected E undoRemoveRecord(CorfuQueue<E> table, long key) {
        return table.mainMap.get(key);
    }

    @DontInstrument
    protected void undoRemove(CorfuQueue<E> queue, E undoRecord, long key) {
        if (undoRecord == null) {
            queue.mainMap.remove(key);
        } else {
            queue.mainMap.put(key, undoRecord);
        }
    }
}
