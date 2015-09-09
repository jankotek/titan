package com.thinkaurelius.titan.diskstorage.berkeleyje;

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

public class BerkeleyJETx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(BerkeleyJETx.class);
    protected static final StaticBuffer TOMBSTONE = new StaticArrayBuffer(new byte[0]);


    private volatile DB db;
    private final Lock commitLock;

    protected ConcurrentNavigableMap<Fun.Pair<String,StaticBuffer>,StaticBuffer> modifiedData =
            new ConcurrentSkipListMap<Fun.Pair<String, StaticBuffer>, StaticBuffer>();

    public BerkeleyJETx(DB db, Lock commitLock,BaseTransactionConfig config) {
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
            Map map;
            for (Map.Entry<Fun.Pair<String,StaticBuffer>, StaticBuffer> e : modifiedData.entrySet()) {
                map = db.treeMap(e.getKey().a);
                StaticBuffer key = e.getKey().b;
                StaticBuffer value = e.getValue();
                if(value==TOMBSTONE)
                    map.remove(key);
                else
                    map.put(key,value);
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
        return (StaticBuffer) db.treeMap(name).get(key);
    }

    public List<KeyValueEntry> getSlice(String name, KeySelector selector,
                                        StaticBuffer keyStart, StaticBuffer keyEnd){

        final List<KeyValueEntry> result = new ArrayList<KeyValueEntry>();
        NavigableMap<StaticBuffer,StaticBuffer> map = db.treeMap(name);
        //get original stuff
        if(map.comparator().compare(keyStart,keyEnd)<0) {
            Set<Map.Entry<StaticBuffer, StaticBuffer>> range = map.subMap(keyStart, keyEnd).entrySet();

            StaticBuffer oldKey = keyStart;

            mainLoop: for (Map.Entry<StaticBuffer, StaticBuffer> e : range) {
                StaticBuffer foundKey = e.getKey();
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
                    value = e.getValue();

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
            if(!db.treeMap(name).containsKey(key)){
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
}