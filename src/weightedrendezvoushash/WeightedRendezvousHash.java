package weightedrendezvoushash;

import java.util.Collection;
import java.util.concurrent.ConcurrentSkipListSet;

import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;

/**
 * <p>A high performance thread safe implementation of Rendezvous (Highest Random Weight, HRW) hashing is an algorithm that allows clients to achieve distributed agreement on which node (or proxy) a given
 * key is to be placed in. This implementation has the following properties.
 * <ul>
 * <li>Non-blocking reads : Determining which node a key belongs to is always non-blocking.  Adding and removing nodes however blocks each other</li>
 * <li>Low overhead: providing using a hash function of low overhead</li>
 * <li>Load balancing: Since the hash function is randomizing, each of the n nodes is equally likely to receive the key K. Loads are uniform across the sites.</li>
 * <li>High hit rate: Since all clients agree on placing an key K into the same node N , each fetch or placement of K into N yields the maximum utility in terms of hit rate. The key K will always be found unless it is evicted by some replacement algorithm at N.</li>
 * <li>Minimal disruption: When a node is removed, only the keys mapped to that node need to be remapped and they will be distributed evenly</li>
 * </ul>
 * </p>
 * source: https://en.wikipedia.org/wiki/Rendezvous_hashing
 *
 * @author Chris Lohfink
 *
 * Weighted version modified by QuyTN
 *
 * @param <K>
 *            type of key
 * @param <N>
 *            type node/site or whatever want to be returned (ie IP address or String)
 */
public class WeightedRendezvousHash<K, N extends Node> {

    /**
     * A hashing function from guava, ie Hashing.murmur3_128()
     */
    private final HashFunction hasher;

    /**
     * A funnel to describe how to take the key and add it to a hash.
     *
     * @see com.google.common.hash.Funnel
     */
    private final Funnel<K> keyFunnel;

    /**
     * Funnel describing how to take the type of the node and add it to a hash
     */
    private final Funnel<N> nodeFunnel;

    /**
     * All the current nodes in the pool
     */
    private final ConcurrentSkipListSet<N> ordered;

    /**
     * Creates a new RendezvousHash with a starting set of nodes provided by init. The funnels will be used when generating the hash that combines the nodes and
     * keys. The hasher specifies the hashing algorithm to use.
     */
    public WeightedRendezvousHash(HashFunction hasher, Funnel<K> keyFunnel, Funnel<N> nodeFunnel, Collection<N> init) {
        if (hasher == null) throw new NullPointerException("hasher");
        if (keyFunnel == null) throw new NullPointerException("keyFunnel");
        if (nodeFunnel == null) throw new NullPointerException("nodeFunnel");
        if (init == null) throw new NullPointerException("init");
        this.hasher = hasher;
        this.keyFunnel = keyFunnel;
        this.nodeFunnel = nodeFunnel;
        this.ordered = new ConcurrentSkipListSet<N>(init);
    }

    /**
     * Removes a node from the pool. Keys that referenced it should after this be evenly distributed amongst the other nodes
     *
     * @return true if the node was in the pool
     */
    public boolean remove(N node) {
        return ordered.remove(node);
    }

    /**
     * Add a new node to pool and take an even distribution of the load off existing nodes
     *
     * @return true if node did not previously exist in pool
     */
    public boolean add(N node) {
        return ordered.add(node);
    }

    /**
     * return a node for a given key
     */
    public N get(K key) {
        double maxScore = -Double.MAX_VALUE;
        N max = null;
        for (N node : ordered) {
            long nodesHash = hasher.newHasher()
                    .putObject(key, keyFunnel)
                    .putObject(node, nodeFunnel)
                    .hash().asLong();
            double score = getScore(nodesHash, node.getWeight());
            if (score > maxScore) {
                max = node;
                maxScore = score;
            }
        }
        return max;
    }

    /**
     * Original python version: https://en.wikipedia.org/wiki/Rendezvous_hashing#Weighted_rendezvous_hash
     *
     * @return adjusted score base on weight
     */
    private double getScore(long hash, int weight) {
        float hash_f = long_to_float(hash);
        double score = 1 / -Math.log(hash_f) ;
        return weight * score;
    }

    /**
     * Original python version: https://en.wikipedia.org/wiki/Rendezvous_hashing#Weighted_rendezvous_hash
     *
     * @return Converts a uniformly random [[64-bit computing|64-bit]] long to uniformly random floating point number on interval <math>[0, 1)</math>.
     */
    private float long_to_float(long value) {
        long fifty_three_ones = 0x0FFFFFFFFFFFFFFFL >> (60 - 53);
        float fifty_three_zeros = (float) (1L << 53);
        return (value & fifty_three_ones) / fifty_three_zeros;
    }
}
