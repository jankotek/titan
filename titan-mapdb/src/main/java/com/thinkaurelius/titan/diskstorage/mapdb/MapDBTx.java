package com.thinkaurelius.titan.diskstorage.mapdb;

import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import org.mapdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.locks.Lock;

public class MapDBTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(MapDBTx.class);
    protected static final StaticBuffer TOMBSTONE = new StaticArrayBuffer(new byte[0]);


    private volatile DB db;
    private final Lock commitLock;

    protected ConcurrentNavigableMap<Fun.Pair<String,StaticBuffer>,StaticBuffer> modifiedData =
            new ConcurrentSkipListMap<Fun.Pair<String, StaticBuffer>, StaticBuffer>();

    public MapDBTx(DB db, Lock commitLock,BaseTransactionConfig config) {
        super(config);
        this.db = db;
        this.commitLock = commitLock;
//        lm = lockMode;
        // tx may be null
//        Preconditions.checkNotNull(lm);
    }

    @Override
    public synchronized void rollback() throws BackendException {
        super.rollback();
        commitLock.lock();
        try {
            if (db == null) return;
            if (log.isTraceEnabled())
                log.trace("{} rolled back", this.toString(), new TransactionClose(this.toString()));

//            closeOpenIterators();
            modifiedData.clear();
            db = null;
        } catch (DBException e) {
            throw new PermanentBackendException(e);
        }finally {
            commitLock.unlock();
        }
    }

    @Override
    public synchronized void commit() throws BackendException {
        super.commit();
        commitLock.lock();
        try {
            if (db == null) return;
            if (log.isTraceEnabled())
                log.trace("{} committed", this.toString(), new TransactionClose(this.toString()));

            //apply modified data
            Map map = null;
            String oldName = "";
            Iterator<Map.Entry<Fun.Pair<String,StaticBuffer>, StaticBuffer>> iter = modifiedData.entrySet().iterator();
            while(iter.hasNext()) {
                Map.Entry<Fun.Pair<String,StaticBuffer>, StaticBuffer> e = iter.next();
                iter.remove();
                if(!oldName.equals(e.getKey().a)) {
                    map = db.treeMap(e.getKey().a);
                    oldName = e.getKey().a;
                }
                StaticBuffer key = e.getKey().b;
                StaticBuffer value = e.getValue();
                if(value==TOMBSTONE)
                    map.remove(toArray(key));
                else
                    map.put(toArray(key),toArray(value));
            }

//            closeOpenIterators();
            db.commit();
            db = null;
        } catch (TxRollbackException e) {
            throw new TemporaryBackendException(e);
        } catch (DBException e) {
            throw new PermanentBackendException(e);
        }finally {
            commitLock.unlock();

        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + (null == db ? "nulltx" : db.toString());
    }

    public StaticBuffer get(String name, StaticBuffer key) {
        StaticBuffer ret = modifiedData.get(new Fun.Pair(name, key));
        if(ret!=null){
            if(ret == TOMBSTONE)
                return null;
            return ret;
        }
        byte[] ret2 = (byte[]) db.treeMap(name).get(toArray(key));
        return ret2==null ? null : new StaticArrayBuffer(ret2);
    }

    public List<KeyValueEntry> getSlice(String name, KeySelector selector,
                                        StaticBuffer keyStart, StaticBuffer keyEnd){

        final List<KeyValueEntry> result = new ArrayList<KeyValueEntry>();
        NavigableMap<byte[],byte[]> map = db.treeMap(name);
        byte[] keyStartB = toArray(keyStart);
        byte[] keyEndB = toArray(keyEnd);
        //get original stuff
        if(map.comparator().compare(keyStartB,keyEndB)<0) {
            Set<Map.Entry<byte[], byte[]>> range = map.subMap(keyStartB, keyEndB).entrySet();

            StaticBuffer oldKey = keyStart;

            mainLoop: for (Map.Entry<byte[], byte[]> e : range) {
                StaticBuffer foundKey = new StaticArrayBuffer(e.getKey());
                if (selector.reachedLimit())
                    break mainLoop;

                //get all modified keys between previous key and this key
                Map<Fun.Pair<String,StaticBuffer>,StaticBuffer> modifiedData2 =
                        modifiedData.subMap(new Fun.Pair(name, oldKey), new Fun.Pair(name, foundKey));

                modLoop:
                for (Map.Entry<Fun.Pair<String,StaticBuffer>, StaticBuffer> e2 : modifiedData2.entrySet()) {
                    if (selector.reachedLimit())
                        break mainLoop;

                    StaticBuffer foundKey2 = e2.getKey().b;
                    if(e2.getValue()==TOMBSTONE)
                        continue modLoop;
                    // and is accepted by selector
                    if (selector.include(foundKey2)) {
                        result.add(new KeyValueEntry(foundKey2, e2.getValue()));
                    }

                    if (selector.reachedLimit())
                        break mainLoop;
                }
                oldKey = foundKey;

                //check if key was not deleted
                StaticBuffer value = modifiedData.get(new Fun.Pair(name,foundKey));
                if(value==TOMBSTONE)
                    continue mainLoop;

                if(value==null)
                    value = new StaticArrayBuffer(e.getValue());

                // and is accepted by selector
                if (selector.include(foundKey)) {
                    result.add(new KeyValueEntry(foundKey, value));
                }

                if (selector.reachedLimit())
                    break mainLoop;
            }

            //now get all modified keys between last key and the end key
            Map<Fun.Pair<String,StaticBuffer>,StaticBuffer> modifiedData2 =
                    modifiedData.subMap(new Fun.Pair(name, oldKey), new Fun.Pair(name, keyEnd));

            modLoop:
            for (Map.Entry<Fun.Pair<String,StaticBuffer>, StaticBuffer> e2 : modifiedData2.entrySet()) {
                if (selector.reachedLimit())
                    break modLoop;

                StaticBuffer foundKey2 = e2.getKey().b;
                if(e2.getValue()==TOMBSTONE)
                    continue modLoop;
                // and is accepted by selector
                if (selector.include(foundKey2)) {
                    result.add(new KeyValueEntry(foundKey2, e2.getValue()));
                }

                if (selector.reachedLimit())
                    break modLoop;
            }
        }


        return result;
    }

    public void insert(String name,StaticBuffer key, StaticBuffer value, boolean allowOverwrite) throws PermanentBackendException {
        if (allowOverwrite){
            modifiedData.put(new Fun.Pair(name, key), value);
        }else {
            //check that key does not exist
            if(!db.treeMap(name).containsKey(toArray(key))){
                Object old = modifiedData.get(new Fun.Pair(name,key));
                if(old!=null && old!=TOMBSTONE){
                    throw new PermanentBackendException("Key already exists on no-overwrite.");
                }
                modifiedData.put(new Fun.Pair(name,key), value);

            }else{
                throw new PermanentBackendException("Key already exists on no-overwrite.");
            }
        }
    }

    public void delete(String name, StaticBuffer key) {
        StaticBuffer old = modifiedData.put(new Fun.Pair(name, key), TOMBSTONE);
//            if (old!=null) {
//                throw new PermanentBackendException("Could not remove");
//            }

    }

    private static class TransactionClose extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionClose(String msg) {
            super(msg);
        }
    }


    static byte[] toArray(StaticBuffer buf) {
        return buf.getBytes(0, buf.length());
    }

}
