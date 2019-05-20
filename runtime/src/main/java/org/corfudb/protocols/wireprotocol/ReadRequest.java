package org.corfudb.protocols.wireprotocol;


import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Created by mwei on 8/11/16.
 */
@Data
@AllArgsConstructor
public class ReadRequest implements ICorfuPayload<ReadRequest> {

    // Requested address to read.
    final long address;

    // Whether the read result should be cached on server.
    final boolean cacheableOnServer;

    /**
     * Deserialization Constructor from ByteBuf to ReadRequest.
     *
     * @param buf The buffer to deserialize
     */
    public ReadRequest(ByteBuf buf) {
        address = buf.readLong();
        cacheableOnServer = buf.readBoolean();
    }

    @Override
    public void doSerialize(ByteBuf buf) {
        buf.writeLong(address);
        buf.writeBoolean(cacheableOnServer);
    }

}
