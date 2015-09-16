package com.thinkaurelius.titan.diskstorage.berkeleyje;

import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.junit.Test;
import org.mapdb.Fun;

import static org.junit.Assert.*;

public class SerializerTest {

    @Test public void byte_Array_comparator(){
        //compare that byte[] is handled the same way in MapDB and titan

        compare(new byte[10], new byte[11]);
        compare(
                new byte[]{ 0,0,0,0, 0, 0, 0, 4, -1, -1, -1, -1, -1, -1, -1, -51, 0, 0, 0, 0, 0, 0, 0, 0},
                new byte[]{0, 0, 0, 0, 0, 0, 0, 4, -1, -1, -1, -1, -1, -1, -1, -51, -1, -1, -1, -1, -1, -1, -1, -1}
        );

    }


    void compare(byte[] b1, byte[] b2){
        assertEquals(
                Math.signum(new StaticArrayBuffer(b1).compareTo(new StaticArrayBuffer(b2))),
                Math.signum(Fun.BYTE_ARRAY_COMPARATOR.compare(b1,b2)),
                0.0001
                );
    }

}