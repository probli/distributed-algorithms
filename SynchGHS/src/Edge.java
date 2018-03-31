import java.util.*;

public class Edge implements Comparable<Edge> {
    int endpoint1;
    int endpoint2;
    int weight;

    public Edge(int uid1, int uid2, int weight) {
        this.endpoint1 = Math.min(uid1, uid2);
        this.endpoint2 = Math.max(uid1, uid2);
        this.weight = weight;
    }

    @Override
    public int compareTo(Edge that) {
        if (this.weight != that.weight) {
            return this.weight - that.weight;
        } else if (this.endpoint1 != that.endpoint1) {
            return this.endpoint1 - that.endpoint1;
        }

        return this.endpoint2 - that.endpoint2;
    }

    @Override
    public String toString() {
        return this.endpoint1 + "," + this.endpoint2 + "," + this.weight;
    }
}
