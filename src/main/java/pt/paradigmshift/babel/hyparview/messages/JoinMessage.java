package pt.paradigmshift.babel.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * First message sent by a joining node to its bootstrap contact: "I am here,
 * please add me to your active view and forward this join". Carries no payload
 * — the {@code from} parameter on the receiving side identifies the joiner.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class JoinMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 2501;

    public JoinMessage() {
        super(MSG_CODE);
    }

    @Override
    public String toString() {
        return "JoinMessage{}";
    }

    public static final ISerializer<JoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinMessage m, ByteBuf out) {
            // empty payload
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) {
            return new JoinMessage();
        }
    };
}
