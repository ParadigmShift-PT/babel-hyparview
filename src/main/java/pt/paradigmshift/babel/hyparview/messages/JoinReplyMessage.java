package pt.paradigmshift.babel.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * Confirmation back to a joiner that the recipient has added it to the
 * recipient's active view. Carries no payload.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class JoinReplyMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 2502;

    public JoinReplyMessage() {
        super(MSG_CODE);
    }

    @Override
    public String toString() {
        return "JoinReplyMessage{}";
    }

    public static final ISerializer<JoinReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinReplyMessage m, ByteBuf out) {
            // empty payload
        }

        @Override
        public JoinReplyMessage deserialize(ByteBuf in) {
            return new JoinReplyMessage();
        }
    };
}
