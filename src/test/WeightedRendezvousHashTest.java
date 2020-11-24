package test;

import com.google.common.collect.Sets;
import com.google.common.hash.*;
import org.junit.Assert;
import org.junit.Test;
import weightedrendezvoushash.WeightedRendezvousHash;

import java.util.ArrayList;
import java.util.Random;

public class WeightedRendezvousHashTest {
    private static final Random rand = new Random();
    private static final HashFunction hfunc = Hashing.murmur3_128();
    private static final Funnel<String> strFunnel = (String from, PrimitiveSink into) -> {
        into.putBytes(from.getBytes());
    };
    private static final Funnel<TestNode> nodeFunnel = (TestNode from, PrimitiveSink into) -> {
        into.putBytes(from.getId().getBytes());
    };

    @Test
    public void testEmpty() {
        WeightedRendezvousHash<String, TestNode> h = genEmpty();
        Assert.assertEquals(null, h.get("key"));
    }

    /**
     * Ensure the same node returned for same key after a large change to the pool of nodes
     */
    @Test
    public void testConsistentAfterRemove() {
        WeightedRendezvousHash<String, TestNode> h = genEmpty();
        for(int i = 0 ; i < 1000; i++) {
            h.add(new TestNode("node"+i, i));
        }
        TestNode node = h.get("key");
        Assert.assertEquals(node, h.get("key"));

        for(int i = 0; i < 250; i++) {
            int randNum = rand.nextInt(1000);
            TestNode toRemove = new TestNode("node" + randNum, randNum);
            if(!toRemove.equals(node)) {
                h.remove(toRemove);
            }
        }
        Assert.assertEquals(node, h.get("key"));
    }

    /**
     * Ensure that a new node returned after deleted
     */
    @Test
    public void testPreviousDeleted() {
        WeightedRendezvousHash<String, TestNode> h = genEmpty();
        TestNode node1 = new TestNode("node1", 1);
        TestNode node2 = new TestNode("node2", 2);
        h.add(node1);
        h.add(node2);
        TestNode node = h.get("key");
        h.remove(node);
        Assert.assertTrue(Sets.newHashSet(node1, node2).contains(h.get("key")));
        Assert.assertTrue(!node.equals(h.get("key")));
    }

    /**
     * Ensure same node will still be returned if removed/read
     */
    @Test
    public void testReAdd() {
        WeightedRendezvousHash<String, TestNode> h = genEmpty();
        TestNode node1 = new TestNode("node1", 1);
        TestNode node2 = new TestNode("node2", 2);
        h.add(node1);
        h.add(node2);
        TestNode node = h.get("key");
        h.remove(node);
        h.add(node);
        Assert.assertEquals(node, h.get("key"));
    }

    /**
     * Ensure 2 hashes if have nodes added in different order will have same results
     */
    @Test
    public void testDifferentOrder() {
        WeightedRendezvousHash<String, TestNode> h = genEmpty();
        WeightedRendezvousHash<String, TestNode> h2 = genEmpty();
        for(int i = 0 ; i < 1000; i++) {
            h.add(new TestNode("node" + i, i));
        }
        for(int i = 999 ; i >= 0; i--) {
            h2.add(new TestNode("node" + i, i));
        }
        Assert.assertEquals(h2.get("key"), h.get("key"));
    }

    @Test
    public void testCollision() {
        HashFunction hfunc = new AlwaysOneHashFunction();
        WeightedRendezvousHash<String, TestNode> h1 = new WeightedRendezvousHash<>(hfunc, strFunnel, nodeFunnel, new ArrayList<>());
        WeightedRendezvousHash<String, TestNode> h2 = new WeightedRendezvousHash<>(hfunc, strFunnel, nodeFunnel, new ArrayList<>());

        for(int i = 0 ; i < 1000; i++) {
            h1.add(new TestNode("node" + i, i));
        }
        for(int i = 999 ; i >= 0; i--) {
            h2.add(new TestNode("node" + i, i));
        }

        Assert.assertEquals(h2.get("key"), h1.get("key"));
    }

    private static WeightedRendezvousHash<String, TestNode> genEmpty() {
        return new WeightedRendezvousHash<>(hfunc, strFunnel, nodeFunnel, new ArrayList<>());
    }
}