package com.thinkaurelius.titan.graphdb.berkeleyje;

import com.thinkaurelius.titan.BerkeleyStorageSetup;
import com.thinkaurelius.titan.diskstorage.configuration.ModifiableConfiguration;
import com.thinkaurelius.titan.diskstorage.configuration.WriteConfiguration;
import com.thinkaurelius.titan.graphdb.TitanGraphTest;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class BerkeleyGraphTest extends TitanGraphTest {

    @Rule
    public TestName methodNameRule = new TestName();

    private static final Logger log =
            LoggerFactory.getLogger(BerkeleyGraphTest.class);

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration mcfg = BerkeleyStorageSetup.getBerkeleyJEConfiguration();
        String methodName = methodNameRule.getMethodName();

        return mcfg.getConfiguration();
    }

    @Override
    @Test
    @Ignore //TODO current mapdb backend does not support it
    public void testConsistencyEnforcement() {
//        // Check that getConfiguration() explicitly set serializable isolation
//        // This could be enforced with a JUnit assertion instead of a Precondition,
//        // but a failure here indicates a problem in the test itself rather than the
//        // system-under-test, so a Precondition seems more appropriate
//        IsolationLevel effective = config.get(ConfigElement.getPath(MapDBStoreManager.ISOLATION_LEVEL), IsolationLevel.class);
//        Preconditions.checkState(IsolationLevel.SERIALIZABLE.equals(effective));
//        super.testConsistencyEnforcement();
    }

    @Override
    protected boolean isLockingOptimistic() {
        return false;
    }

    @Override
    public void testConcurrentConsistencyEnforcement() {
        //Do nothing TODO: Figure out why this is failing in BerkeleyDB!!
    }

    @Override @Before
    public void setUp() throws Exception {
        //delete folder with test
        IOUtils.deleteFromDirectory(new File("target/db"));
        IOUtils.deleteFromDirectory(new File("titan-mapdb/target/db"));
        super.setUp();
    }

    @Test
    @Ignore //TODO current mapdb backend does not support it
    @Override
    public void simpleLogTest() throws InterruptedException {
        super.simpleLogTest();
    }

    @Test
    @Ignore //TODO current mapdb backend does not support it
    @Override
    public void testGlobalOfflineGraphConfig() {
        super.testGlobalOfflineGraphConfig();
    }

    @Test
    @Ignore //TODO current mapdb backend does not support it
    @Override
    public void simpleLogTestWithFailure() throws InterruptedException {
        super.simpleLogTestWithFailure();
    }

    @Test
    @Ignore //TODO current mapdb backend does not support it
    @Override
    public void testIndexUpdateSyncWithMultipleInstances() throws InterruptedException {
        super.testIndexUpdateSyncWithMultipleInstances();
    }


}
