package pt.paradigmshift.babel.hyparview.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import pt.unl.fct.di.novasys.network.data.Host;

/**
 * Standard {@link IView} backed by a {@link HashSet} plus a shared
 * {@link Random} for sampling. The view refuses to add peers that are
 * already in the local host, the other view, or the shared pending set.
 *
 * <p>Random samples are produced by Fisher-Yates partial-shuffle, replacing
 * the upstream's O(n²) "remove random index until size matches" pattern.
 *
 * <p>This class is not thread-safe; all access goes through Babel's
 * single-threaded protocol event loop.
 */
public class View implements IView {

    private final int capacity;
    private final Set<Host> peers;
    private final Random rnd;
    private final Host self;

    private IView other;
    private Set<Host> pending;

    public View(int capacity, Host self, Random rnd) {
        this.capacity = capacity;
        this.self = self;
        this.peers = new HashSet<>();
        this.rnd = rnd;
    }

    @Override
    public int getCapacity() {
        return capacity;
    }

    @Override
    public void setOther(IView other, Set<Host> pending) {
        this.other = other;
        this.pending = pending;
    }

    @Override
    public Host addPeer(Host peer) {
        if (peer.equals(self) || peers.contains(peer) || other.containsPeer(peer) || pending.contains(peer)) {
            return null;
        }
        Host evicted = null;
        if (peers.size() == capacity) {
            evicted = dropRandom();
        }
        peers.add(peer);
        assert peers.size() <= capacity;
        return evicted;
    }

    @Override
    public boolean removePeer(Host peer) {
        return peers.remove(peer);
    }

    @Override
    public boolean containsPeer(Host peer) {
        return peers.contains(peer);
    }

    @Override
    public Host dropRandom() {
        if (peers.isEmpty()) {
            return null;
        }
        // Iterate to a random index. peers is a HashSet so we cannot index
        // directly; copying to an array once is the cheapest correct approach.
        Host[] hosts = peers.toArray(new Host[0]);
        Host victim = hosts[rnd.nextInt(hosts.length)];
        peers.remove(victim);
        return victim;
    }

    @Override
    public Set<Host> getRandomSample(int sampleSize) {
        if (peers.size() <= sampleSize) {
            return new HashSet<>(peers);
        }
        // Fisher-Yates partial shuffle on a copy: O(n + sampleSize) instead of
        // the upstream's O(n^2) repeated-remove-by-index.
        List<Host> hosts = new ArrayList<>(peers);
        for (int i = 0; i < sampleSize; i++) {
            int j = i + rnd.nextInt(hosts.size() - i);
            Collections.swap(hosts, i, j);
        }
        return new HashSet<>(hosts.subList(0, sampleSize));
    }

    @Override
    public Set<Host> getPeers() {
        return peers;
    }

    @Override
    public Host getRandom() {
        if (peers.isEmpty()) {
            return null;
        }
        Host[] hosts = peers.toArray(new Host[0]);
        return hosts[rnd.nextInt(hosts.length)];
    }

    @Override
    public Host getRandomDiff(Host from, Host from2) {
        List<Host> hosts = new ArrayList<>(peers);
        if (from != null) hosts.remove(from);
        if (from2 != null) hosts.remove(from2);
        return hosts.isEmpty() ? null : hosts.get(rnd.nextInt(hosts.size()));
    }

    @Override
    public boolean fullWithPending(Set<Host> pending) {
        assert peers.size() + pending.size() <= capacity;
        return peers.size() + pending.size() >= capacity;
    }

    @Override
    public boolean isFull() {
        return peers.size() >= capacity;
    }

    @Override
    public String toString() {
        return "View{peers=" + peers + '}';
    }
}
