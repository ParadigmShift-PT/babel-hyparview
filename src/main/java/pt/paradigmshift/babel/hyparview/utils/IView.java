package pt.paradigmshift.babel.hyparview.utils;

import java.util.Set;

import pt.unl.fct.di.novasys.network.data.Host;

/**
 * Abstract operations on one of the two HyParView views (active / passive).
 * Concrete instances of {@link View} are wired together via
 * {@link #setOther(IView, Set)} so each view can refuse adding a peer that
 * is already in the other view or in the shared pending set.
 */
public interface IView {

    /** @return the capacity (maximum size) of this view */
    int getCapacity();

    /**
     * Wire this view to its sibling view (the other of active/passive) plus
     * the protocol's pending set, so {@link #addPeer(Host)} can refuse peers
     * already known there.
     */
    void setOther(IView other, Set<Host> pending);

    /**
     * Add {@code peer} to this view, unless it is the local host, already
     * present here, present in the other view, or pending. If the view is
     * full, drops a random existing peer to make room and returns that
     * peer; otherwise returns {@code null}.
     *
     * @param peer the peer to add
     * @return the evicted peer, or {@code null} if no eviction happened
     */
    Host addPeer(Host peer);

    /** @return {@code true} if the peer was present and removed */
    boolean removePeer(Host peer);

    /** @return {@code true} if the peer is currently in this view */
    boolean containsPeer(Host peer);

    /** Drop and return a random peer from this view, or {@code null} if empty. */
    Host dropRandom();

    /** @return a random sample of size {@code min(capacity, sampleSize)} from this view */
    Set<Host> getRandomSample(int sampleSize);

    /** @return the underlying peer set (live view — do not modify externally) */
    Set<Host> getPeers();

    /** @return a random peer from this view, or {@code null} if empty */
    Host getRandom();

    /**
     * @return a random peer from this view, excluding {@code from} and
     *         {@code from2} (either may be {@code null}); {@code null} if no
     *         such peer exists
     */
    Host getRandomDiff(Host from, Host from2);

    /**
     * @return {@code true} when this view's size plus the {@code pending} set
     *         size has reached capacity (no new peers can be added without
     *         eviction)
     */
    boolean fullWithPending(Set<Host> pending);

    /** @return {@code true} when this view is at full capacity */
    boolean isFull();
}
