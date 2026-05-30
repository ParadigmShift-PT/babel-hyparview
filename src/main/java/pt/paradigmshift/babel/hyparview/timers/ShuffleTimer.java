package pt.paradigmshift.babel.hyparview.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

/**
 * Periodic timer that triggers one shuffle round in
 * {@link pt.paradigmshift.babel.hyparview.HyParView}.
 *
 * <p>Handler class: timer. <b>ID:</b> {@value #TIMER_CODE}.
 */
public class ShuffleTimer extends ProtoTimer {

    /** Babel timer numeric identifier. */
    public static final short TIMER_CODE = 2501;

    public ShuffleTimer() {
        super(TIMER_CODE);
    }

    /** Stateless timer: returns {@code this}. */
    @Override
    public ProtoTimer clone() {
        return this;
    }
}
