package edu.rice.comp322.boruvka.parallel;

import edu.rice.comp322.AbstractBoruvka;
import edu.rice.comp322.boruvka.BoruvkaFactory;
import edu.rice.comp322.boruvka.Edge;
import edu.rice.comp322.boruvka.Loader;
import edu.rice.hj.api.SuspendableException;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.*;

/**
 * This class must be modified.
 *
 * @author <a href="http://shams.web.rice.edu/">Shams Imam</a> (shams@rice.edu)
 */
public final class ParBoruvka extends AbstractBoruvka implements BoruvkaFactory<ParComponent, ParEdge> {

    protected final ConcurrentLinkedQueue<ParComponent> nodesLoaded = new ConcurrentLinkedQueue<>();
    ParComponent result = null;

    public ParBoruvka() {
        super();
    }

    @Override
    public boolean usesHjLib() {
        return false;
    }

    @Override
    public void initialize(final String[] args) {
        Loader.parseArgs(args);
    }

    @Override
    public void preIteration(final int iterationIndex) {
        // Exclude reading file input from timing measurement
        nodesLoaded.clear();
        Loader.read(this, nodesLoaded);

        totalEdges = 0;
        totalWeight = 0;
    }

    @Override
    public void runIteration(int nthreads) throws SuspendableException {
        computeBoruvka(nodesLoaded, nthreads);
    }

    /**
     * NOTES:
     *      - If two iterations of the while loop work on disjoint (n, other) pairs, then the iterations can be
     * executed in parallel using a thread-safe implementation of the work-list that allows inserts and
     * removes to be invoked in parallel.
     *          - maybe make a helper to determine if they are disjoint?
     */
    private void computeBoruvka(final Queue<ParComponent> nodesLoaded, int nthreads) {

        final Thread[] threads = new Thread[nthreads]; /* Array of threads */

        for (int i = 0; i < nthreads; i++) {    /* Iterate over threads */

            threads[i] = new Thread(() -> { /* Initialize new thread */

                ParComponent cur = null;    /* Current node */

                while ((cur = nodesLoaded.poll()) != null) {

                    /* Another thread may be working on this node */
                    if (!cur.lock.tryLock()) {
                        continue;
                    }

                    /* Make sure current node is not already processed */
                    if (cur.isDead) {
                        cur.lock.unlock();
                        continue;
                    }

                    /* Retrieve loopNode's edge with minimum cost */
                    final Edge<ParComponent> min_edge = cur.getMinEdge();

                    /* No minimum edge. We have contracted the graph! */
                    if (min_edge == null) {
                        result = cur;    /* Result is current node */
                        break;  /* MST found - break out of for loop */
                    }

                    /* Get neighbor node */
                    final ParComponent neighbor_node = min_edge.getOther(cur);


                    /* Cannot merge if we cannot unlock neighbor node */
                    if (!neighbor_node.lock.tryLock()) {
                        cur.lock.unlock();  /* Release current node's lock */
                        nodesLoaded.add(cur);   /* Add current node to work list */
                        continue;   /* Continue to next thread */
                    }

                    /* Check if neighbor node has already been processed */
                    if (neighbor_node.isDead) {
                        neighbor_node.lock.unlock(); /* Unlock both nodes */
                        cur.lock.unlock();
                        nodesLoaded.add(cur);   /* Current node has been processed */
                        continue; /* Continue to next thread */
                    }

                    /* If we get here, we can safely merge */
                    neighbor_node.isDead = true;
                    cur.merge(neighbor_node, min_edge.weight()); /* Merge current and neighbor nodes */
                    cur.lock.unlock();  /* Unlock both nodes */
                    neighbor_node.lock.unlock();
                    nodesLoaded.add(cur);   /* Current node has been processed */
                }
            });

            threads[i].start(); /* Safely start this thread */
        }

        for (int i = 0; i < nthreads; i++) {
            try {
                threads[i].join();  /* Wait for thread and join */
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // END OF EDGE CONTRACTION ALGORITHM
        if (result != null) {
            totalEdges = result.totalEdges();
            totalWeight = result.totalWeight();

        }
    }

    @Override
    public ParComponent newComponent(final int nodeId) {
        return new ParComponent(nodeId);
    }

    @Override
    public ParEdge newEdge(final ParComponent from, final ParComponent to, final double weight) {
        return new ParEdge(from, to, weight);
    }
}


