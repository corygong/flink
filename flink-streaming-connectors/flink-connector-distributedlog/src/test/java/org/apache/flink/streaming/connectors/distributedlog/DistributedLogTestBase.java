package org.apache.flink.streaming.connectors.distributedlog;

import com.twitter.distributedlog.*;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.shims.zk.ZooKeeperServerShim;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class DistributedLogTestBase {
	static final Logger LOG = LoggerFactory.getLogger(DistributedLogTestBase.class);

    // Num worker threads should be one, since the exec service is used for the ordered
    // future pool in test cases, and setting to > 1 will therefore result in unordered
    // write ops.
    protected static DistributedLogConfiguration conf =
        new DistributedLogConfiguration()
                .setEnableReadAhead(true)
                .setReadAheadMaxRecords(1000)
                .setReadAheadBatchSize(10)
                .setLockTimeout(1)
                .setNumWorkerThreads(1)
                .setReadAheadNoSuchLedgerExceptionOnReadLACErrorThresholdMillis(20)
                .setSchedulerShutdownTimeoutMs(0)
                .setDLLedgerMetadataLayoutVersion(LogSegmentMetadata.LEDGER_METADATA_CURRENT_LAYOUT_VERSION);
    protected ZooKeeper zkc;
    protected static LocalDLMEmulator bkutil;
    protected static ZooKeeperServerShim zks;
    protected static String zkServers;
    protected static int zkPort;
    protected static int numBookies = 3;
    protected static final List<File> tmpDirs = new ArrayList<File>();

    @BeforeClass
    public static void setupCluster() throws Exception {
        File zkTmpDir = IOUtils.createTempDir("zookeeper", "distrlog");
        tmpDirs.add(zkTmpDir);
        Pair<ZooKeeperServerShim, Integer> serverAndPort = LocalDLMEmulator.runZookeeperOnAnyPort(zkTmpDir);
        zks = serverAndPort.getLeft();
        zkPort = serverAndPort.getRight();
        bkutil = LocalDLMEmulator.newBuilder()
                .numBookies(numBookies)
                .zkHost("127.0.0.1")
                .zkPort(zkPort)
                .serverConf(loadTestBkConf())
                .shouldStartZK(false)
                .build();
        bkutil.start();
        zkServers = "127.0.0.1:" + zkPort;
    }

	public static ServerConfiguration loadTestBkConf() {
		ServerConfiguration conf = new ServerConfiguration();
		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		URL confUrl = classLoader.getResource("bk_server.conf");
		try {
			if (null != confUrl) {
				conf.loadConf(confUrl);
				LOG.info("loaded bk_server.conf from resources");
			}
		} catch (org.apache.commons.configuration.ConfigurationException ex) {
			LOG.warn("loading conf failed", ex);
		}
		return conf;
	}

    @AfterClass
    public static void teardownCluster() throws Exception {
        bkutil.teardown();
        zks.stop();
        for (File dir : tmpDirs) {
            FileUtils.deleteDirectory(dir);
        }
    }

    @Before
    public void setup() throws Exception {
        try {
            zkc = LocalDLMEmulator.connectZooKeeper("127.0.0.1", zkPort);
        } catch (Exception ex) {
            LOG.error("hit exception connecting to zookeeper at {}:{}", new Object[] { "127.0.0.1", zkPort, ex });
            throw ex;
        }
    }

    @After
    public void teardown() throws Exception {
        if (null != zkc) {
            zkc.close();
        }
    }

    protected LogRecord waitForNextRecord(LogReader reader) throws Exception {
        LogRecord record = reader.readNext(false);
        while (null == record) {
            record = reader.readNext(false);
        }
        return record;
    }

    public URI createDLMURI(String path) throws Exception {
        return  LocalDLMEmulator.createDLMURI("127.0.0.1:" + zkPort, path);
    }

    protected void ensureURICreated(URI uri) throws Exception {
        ensureURICreated(zkc, uri);
    }

    protected void ensureURICreated(ZooKeeper zkc, URI uri) throws Exception {
        try {
            zkc.create(uri.getPath(), new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException nee) {
            // ignore
        }
    }
}
