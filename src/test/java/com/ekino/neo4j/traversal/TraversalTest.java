package com.ekino.neo4j.traversal;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

import static java.util.stream.IntStream.range;

/**
 * @author mh
 * @since 19.02.16
 */
public class TraversalTest {

    private static final int RUNS = 10;
    public static final String MSG = "3G heap 2.2 breadth-first";
    public static final String STORE_DIR = "target/test.db";
    private PrintStream log;
    private GraphDatabaseService db;
    private TrueBNodesCounter counter;

    @org.junit.Before
    public void setUp() throws Exception {
        log = new PrintStream(new FileOutputStream("run.log",true));
//        FileUtils.deleteRecursively(new File(STORE_DIR));
        boolean exists = new File(STORE_DIR).exists();
        db = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(STORE_DIR)
        .setConfig(GraphDatabaseSettings.allow_store_upgrade,"true")
        .setConfig(GraphDatabaseSettings.pagecache_memory,"500M")
        .setConfig(GraphDatabaseSettings.cache_type,"weak")
                .newGraphDatabase();
        if (!exists) new PopulateResource(db).populate(null,null);
        counter = new TrueBNodesCounter(db);
        runTraverse(2);
    }

    @Test
    public void testTraverse() throws Exception {
        runTraverse(RUNS);
    }

    private void runTraverse(int runs) {
        String msg="";
        long start = System.nanoTime();
        int sum = 0;
        try {
            sum = range(0, runs).parallel().map((i) -> {
                try (Transaction tx = db.beginTx()) {
                    return counter.count();
                }
            }).sum();
        } catch(Throwable e) {
            msg = e.getMessage();
            e.printStackTrace();
        } finally {
            long end = System.nanoTime();
            long delta = TimeUnit.NANOSECONDS.toMillis(end - start);
            String logMessage = String.format(MSG+": Traversing %d times took %d ms %d ms per run resulting in %d %s%n", runs, delta, delta / runs,sum/runs,msg);
            System.out.print(logMessage);
            log.print(logMessage);log.flush();
        }
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }
}
