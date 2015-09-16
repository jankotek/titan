package com.thinkaurelius.titan.diskstorage.mapdb;

import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeySelector;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStore;
import com.thinkaurelius.titan.diskstorage.util.RecordIterator;
import org.mapdb.DBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MapDBKeyValueStore implements OrderedKeyValueStore {

    private static final Logger log = LoggerFactory.getLogger(MapDBKeyValueStore.class);

    private final String name;
    private final MapDBStoreManager manager;
    private boolean isOpen;

    public MapDBKeyValueStore(String n, MapDBStoreManager m) {
        name = n;
        manager = m;
        isOpen = true;
    }


    @Override
    public String getName() {
        return name;
    }


    @Override
    public synchronized void close() throws BackendException {
        if (isOpen) manager.removeDatabase(this);
        isOpen = false;
    }

    @Override
    public StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException {

        log.trace("db={}, op=get, tx={}", name, txh);
        return ((MapDBTx)txh).get(name,key);
    }

    @Override
    public boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException {
        return get(key,txh)!=null;
    }

    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
    }


    @Override
    public RecordIterator<KeyValueEntry> getSlice(KVQuery query, StoreTransaction txh) throws BackendException {
        log.trace("beginning db={}, op=getSlice, tx={}", name, txh);

        final StaticBuffer keyStart = query.getStart();
        final StaticBuffer keyEnd = query.getEnd();
        final KeySelector selector = query.getKeySelector();

        try {
            final List<KeyValueEntry> result = ((MapDBTx)txh).getSlice(name,selector,keyStart,keyEnd);

            log.trace("db={}, op=getSlice, tx={}, resultcount={}", name, txh, result.size());
//            log.trace("db={}, op=getSlice, tx={}, resultcount={}", name, txh, result.size(), new Throwable("getSlice trace"));

            return new RecordIterator<KeyValueEntry>() {
                private final Iterator<KeyValueEntry> entries = result.iterator();

                @Override
                public boolean hasNext() {
                    return entries.hasNext();
                }

                @Override
                public KeyValueEntry next() {
                    return entries.next();
                }

                @Override
                public void close() {
                }

                @Override
                public void remove() {
                    throw new UnsupportedOperationException();
                }
            };
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public Map<KVQuery,RecordIterator<KeyValueEntry>> getSlices(List<KVQuery> queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh) throws BackendException {
        insert(key, value, txh, true);
    }

    public void insert(StaticBuffer key, StaticBuffer value, StoreTransaction txh, boolean allowOverwrite) throws BackendException {
        try {
            log.trace("db={}, op=insert, tx={}", name, txh);

            ((MapDBTx)txh).insert(name,key,value, allowOverwrite);


        } catch (DBException e) {
            throw new PermanentBackendException(e);
        }
    }


    @Override
    public void delete(StaticBuffer key, StoreTransaction txh) throws BackendException {
        log.trace("Deletion");
        try {
            log.trace("db={}, op=delete, tx={}", name, txh);
            ((MapDBTx)txh).delete(name, key);
        } catch (DBException e) {
            throw new PermanentBackendException(e);
        }
    }

}
