package com.thinkaurelius.titan.diskstorage.mapdb;


import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.common.LocalStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.configuration.MergedConfiguration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.KeyValueEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.mapdb.DB;
import org.mapdb.DBException;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


@PreInitializeConfigOptions
public class MapDBStoreManager extends LocalStoreManager implements OrderedKeyValueStoreManager {

    private static final Logger log = LoggerFactory.getLogger(MapDBStoreManager.class);

    public static final ConfigNamespace MAPDB_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "mapdb", "MapDB configuration options");

    public static final ConfigOption<Integer> NODE_SIZE =
            new ConfigOption<Integer>(MAPDB_NS,"nodeSize",
            "Maximal BTree node size",
            ConfigOption.Type.MASKABLE, 32, ConfigOption.positiveInt());

    public static final ConfigOption<Boolean> VALUES_OUTSIDE_NODE =
            new ConfigOption<Boolean>(MAPDB_NS,"valuesOutsideNode",
                    "If values should be stored outside of BTree nodes",
                    ConfigOption.Type.LOCAL, false);


    public static final ConfigOption<Integer> CACHE_SIZE =
            new ConfigOption<Integer>(MAPDB_NS,"cachesize",
                    "Size of cache for MapDB store",
                    ConfigOption.Type.MASKABLE, 1024*4, ConfigOption.positiveInt());


    private final Map<String, MapDBKeyValueStore> stores;

    protected DB db;
    protected final Lock commitLock = new ReentrantLock();
//    protected Environment environment;
    protected final StoreFeatures features;
    protected final boolean valuesOutsideNodes;
    protected final int nodeSize;

    public MapDBStoreManager(Configuration configuration) throws BackendException {
        super(configuration);
        stores = new HashMap<String, MapDBKeyValueStore>();

        int cacheSize = configuration.get(CACHE_SIZE);
        valuesOutsideNodes = configuration.get(VALUES_OUTSIDE_NODE);
        nodeSize = configuration.get(NODE_SIZE);


        features = new StandardStoreFeatures.Builder()
                    .orderedScan(true)
                    .transactional(transactional)
                    .keyConsistent(GraphDatabaseConfiguration.buildConfiguration())
                    .locking(true)
                    .keyOrdered(true)
                    .build();

        DBMaker.Maker maker =  DBMaker.fileDB(new File(directory, "titanBackEnd.mapdb"))
                .fileLockDisable()
                .fileMmapEnableIfSupported()
                .cacheHashTableEnable(cacheSize)
                .serializerClassLoader(MapDBStoreManager.class.getClassLoader());

        if(batchLoading) {
            maker.transactionDisable();
            maker.asyncWriteEnable();
        }

        db = maker.make();



//        features = new StoreFeatures();
//        features.supportsOrderedScan = true;
//        features.supportsUnorderedScan = false;
//        features.supportsBatchMutation = false;
//        features.supportsTxIsolation = transactional;
//        features.supportsConsistentKeyOperations = true;
//        features.supportsLocking = true;
//        features.isKeyOrdered = true;
//        features.isDistributed = false;
//        features.hasLocalKeyPartition = false;
//        features.supportsMultiQuery = false;
    }


    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public MapDBTx beginTransaction(final BaseTransactionConfig txCfg) throws BackendException {
        try {


            Configuration effectiveCfg =
                    new MergedConfiguration(txCfg.getCustomOptions(), getStorageConfig());

            if (transactional) {
//                TransactionConfig txnConfig = new TransactionConfig();
//                effectiveCfg.get(ISOLATION_LEVEL).configure(txnConfig);
//                tx = environment.beginTransaction(null, txnConfig);
            }
            MapDBTx btx = new MapDBTx(db, commitLock, txCfg);

            if (log.isTraceEnabled()) {
                log.trace("Berkeley tx created", new TransactionBegin(btx.toString()));
            }

            return btx;
        } catch (DBException e) {
            throw new PermanentBackendException("Could not start BerkeleyJE transaction", e);
        }
    }

    @Override
    public MapDBKeyValueStore openDatabase(String name) throws BackendException {
        Preconditions.checkNotNull(name);
        if (stores.containsKey(name)) {
            MapDBKeyValueStore store = stores.get(name);
            return store;
        }
        try {
            //make sure map exists
            if(db.get(name)==null) {
                DB.BTreeMapMaker maker = db.treeMapCreate(name)
                        .keySerializer(Serializer.BYTE_ARRAY)
                        .valueSerializer(Serializer.BYTE_ARRAY)
                        .nodeSize(nodeSize);

                if(valuesOutsideNodes)
                    maker.valuesOutsideNodesEnable();
                maker.makeOrGet();


                db.commit();
            }

            log.debug("Opened database {}", name, new Throwable());

            MapDBKeyValueStore store = new MapDBKeyValueStore(name, this);
            stores.put(name, store);
            return store;
        } catch (DBException e) {
            throw new PermanentBackendException("Could not open BerkeleyJE data store", e);
        }
    }

    @Override
    public void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String,KVMutation> muts : mutations.entrySet()) {
            MapDBKeyValueStore store = openDatabase(muts.getKey());
            KVMutation mut = muts.getValue();

            if (!mut.hasAdditions() && !mut.hasDeletions()) {
                log.debug("Empty mutation set for {}, doing nothing", muts.getKey());
            } else {
                log.debug("Mutating {}", muts.getKey());
            }

            if (mut.hasAdditions()) {
                for (KeyValueEntry entry : mut.getAdditions()) {
                    store.insert(entry.getKey(),entry.getValue(),txh);
                    log.trace("Insertion on {}: {}", muts.getKey(), entry);
                }
            }
            if (mut.hasDeletions()) {
                for (StaticBuffer del : mut.getDeletions()) {
                    store.delete(del,txh);
                    log.trace("Deletion on {}: {}", muts.getKey(), del);
                }
            }
        }
    }

    void removeDatabase(MapDBKeyValueStore db) {
        if (!stores.containsKey(db.getName())) {
            throw new IllegalArgumentException("Tried to remove an unkown database from the storage manager");
        }
        String name = db.getName();
        stores.remove(name);
        log.debug("Removed database {}", name);
    }


    @Override
    public void close() throws BackendException {
        if (db != null) {
            if (!stores.isEmpty())
                throw new IllegalStateException("Cannot shutdown manager since some databases are still open");
            try {
                // TODO this looks like a race condition
                //Wait just a little bit before closing so that independent transaction threads can clean up.
                Thread.sleep(30);
            } catch (InterruptedException e) {
                //Ignore
            }
            try {
                db.close();
                db=null;
            } catch (DBException e) {
                throw new PermanentBackendException("Could not close BerkeleyJE database", e);
            }
        }

    }

    @Override
    public void clearStorage() throws BackendException {
        if (!stores.isEmpty())
            throw new IllegalStateException("Cannot delete store, since database is open: " + stores.keySet().toString());


//        Transaction tx = null;
//        for (String db : environment.getDatabaseNames()) {
//            environment.removeDatabase(tx, db);
//            log.debug("Removed database {} (clearStorage)", db);
//        }
        close();
        IOUtils.deleteFromDirectory(directory);
        if(directory.listFiles().length!=0)
            throw new AssertionError("could not clear storage");
    }

    @Override
    public String getName() {
        return getClass().getSimpleName() + ":" + directory.toString();
    }


    public static enum IsolationLevel {
        READ_UNCOMMITTED {
//            @Override
//            void configure(TransactionConfig cfg) {
//                cfg.setReadUncommitted(true);
//            }
        }, READ_COMMITTED {
//            @Override
//            void configure(TransactionConfig cfg) {
//                cfg.setReadCommitted(true);
//
//            }
        }, REPEATABLE_READ {
//            @Override
//            void configure(TransactionConfig cfg) {
//                // This is the default and has no setter
//            }
        }, SERIALIZABLE {
//            @Override
//            void configure(TransactionConfig cfg) {
//                cfg.setSerializableIsolation(true);
//            }
        };

//        abstract void configure(TransactionConfig cfg);
    };

    private static class TransactionBegin extends Exception {
        private static final long serialVersionUID = 1L;

        private TransactionBegin(String msg) {
            super(msg);
        }
    }
}
