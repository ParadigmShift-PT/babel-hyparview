package pt.paradigmshift.babel.hyparview.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * One-shot timer used by {@link pt.paradigmshift.babel.hyparview.HyParView} to
 * re-check whether the active view is at capacity after a recent
 * {@code NeighborDown} or failed promotion. Fires with an exponentially
 * increasing delay up to 60 s.
 *
 * <p>Handler class: timer. <b>ID:</b> {@value #TIMER_CODE}.
 */
public class CheckConnectivityTimeout extends ProtoTimer {

    /** Babel timer numeric identifier. */
    public static final short TIMER_CODE = 2502;

    public CheckConnectivityTimeout() {
        super(TIMER_CODE);
    }

    /** Stateless timer: returns {@code this}. */
    @Override
    public ProtoTimer clone() {
        return this;
    }
}
