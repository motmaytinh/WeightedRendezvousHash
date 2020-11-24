package test;

import weightedrendezvoushash.Node;

public class TestNode extends Node {
    private final String id;

    TestNode(String id, int weight) {
        super(weight);
        this.id = id;
    }

    public String getId() {
        return id;
    }

    public String getKey() {
        return getId() + "_" + getWeight();
    }

    @Override
    public int compareTo(Object other) {
        String thisKey = this.getKey();
        String theOtherKey = ((TestNode) other).getKey();
        return thisKey.hashCode() - theOtherKey.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return ((TestNode) obj).getKey().equals(this.getKey());
    }
}
