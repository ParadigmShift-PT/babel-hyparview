package pt.paradigmshift.babel.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * Notifies a peer that the sender has dropped it from the sender's active
 * view (typically because the sender chose a different peer for that slot in
 * an active-view rebalance). The receiver should move the sender into its
 * passive view.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class DisconnectMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 2504;

    public DisconnectMessage() {
        super(MSG_CODE);
    }

    @Override
    public String toString() {
        return "DisconnectMessage{}";
    }

    public static final ISerializer<DisconnectMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(DisconnectMessage m, ByteBuf out) {
            // empty payload
        }

        @Override
        public DisconnectMessage deserialize(ByteBuf in) {
            return new DisconnectMessage();
        }
    };
}
