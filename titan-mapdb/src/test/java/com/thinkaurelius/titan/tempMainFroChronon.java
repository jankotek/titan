package com.thinkaurelius.titan;

import com.thinkaurelius.titan.blueprints.BerkeleyBlueprintsTest;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.graphdb.blueprints.TitanBlueprintsGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.GraphTest;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Tomer Sagi on 24-Jul-15.
 * Main class to be used by Chronon which cannot be run inside maven exec / test
 */
public class tempMainFroChronon {

    /**
     *
     * @param args args[0]=1 simple, 2 competing threads
     */
    public static void main(String args[]) {

        if (args.length==0)
            System.exit(-1);

        final GraphTest graphTest = new BerkeleyBlueprintsTest();
        final TitanBlueprintsGraph graph = (TitanBlueprintsGraph) graphTest.generateGraph();



        if (args[0].equals("1")) {

            //Need to define types before hand to avoid deadlock in transactions

            TitanManagement mgmt = graph.getManagementSystem();
            mgmt.makeEdgeLabel("friend").make();
            mgmt.makePropertyKey("test").dataType(Long.class).make();
            mgmt.makePropertyKey("blah").dataType(Float.class).make();
            mgmt.makePropertyKey("bloop").dataType(Integer.class).make();


            mgmt.commit();
            graph.shutdown();

        } else if (args[0].equals("2")) {
            int totalThreads = 4;
            final AtomicInteger vertices = new AtomicInteger(0);
            final AtomicInteger edges = new AtomicInteger(0);
            final AtomicInteger completedThreads = new AtomicInteger(0);
            for (int i = 0; i < totalThreads; i++) {
                new Thread() {
                    public void run() {
                        System.out.println("Started thread");
                        Random random = new Random();
                        if (random.nextBoolean()) {
                            Vertex a = graph.addVertex(null);
                            Vertex b = graph.addVertex(null);
                            Edge e = graph.addEdge(null, a, b, graphTest.convertLabel("friend"));

                            if (graph.getFeatures().supportsElementProperties()) {
                                a.setProperty("test", this.getId());
                                b.setProperty("blah", random.nextFloat());
                                e.setProperty("bloop", random.nextInt());
                            }
                            vertices.getAndAdd(2);
                            edges.getAndAdd(1);
                            graph.commit();
                        } else {
                            Vertex a = graph.addVertex(null);
                            Vertex b = graph.addVertex(null);
                            Edge e = graph.addEdge(null, a, b, graphTest.convertLabel("friend"));
                            if (graph.getFeatures().supportsElementProperties()) {
                                a.setProperty("test", this.getId());
                                b.setProperty("blah", random.nextFloat());
                                e.setProperty("bloop", random.nextInt());
                            }
                            if (random.nextBoolean()) {
                                graph.commit();
                                vertices.getAndAdd(2);
                                edges.getAndAdd(1);
                            } else {
                                graph.rollback();
                            }
                        }
                        completedThreads.getAndAdd(1);
                        System.out.println("Finished thread");
                    }
                }.start();
            }
        }
        System.out.println("Main exit");
    }
}
