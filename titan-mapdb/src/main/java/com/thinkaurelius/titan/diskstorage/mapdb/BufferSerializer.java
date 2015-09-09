package com.thinkaurelius.titan.diskstorage.mapdb;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.mapdb.DataIO;
import org.mapdb.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

final class BufferSerializer extends Serializer<StaticBuffer> implements Serializable {

    private static final StaticBuffer.Factory<byte[]> BYTE_ARRAY_FACTORY = new StaticBuffer.Factory<byte[]>() {
        @Override
        public byte[] get(byte[] array, int offset, int limit) {
            return Arrays.copyOfRange(array, offset, limit);
        }
    };

    @Override
        public void serialize(DataOutput out, StaticBuffer value) throws IOException {
            byte[] b = value.as(BYTE_ARRAY_FACTORY);
            DataIO.packInt(out, b.length);
            out.write(b);
        }

        @Override
        public StaticBuffer deserialize(DataInput in, int available) throws IOException {
            int len = DataIO.unpackInt(in);
            byte[] b = new byte[len];
            in.readFully(b);
            return new StaticArrayBuffer(b);
        }
    }
