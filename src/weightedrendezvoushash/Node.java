package weightedrendezvoushash;

/**
 *
 * @author quytn
 */
public abstract class Node implements Comparable {
    private int weight;

    public Node(int weight) {
        this.weight = weight;
    }

    public int getWeight() {
        return weight;
    }
}