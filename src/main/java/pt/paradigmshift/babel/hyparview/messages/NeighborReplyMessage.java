package pt.paradigmshift.babel.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * Reply to a {@link NeighborRequestMessage}. {@code accepted=true} means
 * "I have added you to my active view and you may consider me a peer";
 * {@code accepted=false} means "I refuse — my active view is full".
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class NeighborReplyMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 2506;

    private final boolean accepted;

    public NeighborReplyMessage(boolean accepted) {
        super(MSG_CODE);
        this.accepted = accepted;
    }

    /** @return {@code true} if the recipient accepted the request */
    public boolean isAccepted() {
        return accepted;
    }

    @Override
    public String toString() {
        return "NeighborReplyMessage{accepted=" + accepted + '}';
    }

    public static final ISerializer<NeighborReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NeighborReplyMessage m, ByteBuf out) {
            out.writeBoolean(m.accepted);
        }

        @Override
        public NeighborReplyMessage deserialize(ByteBuf in) {
            return new NeighborReplyMessage(in.readBoolean());
        }
    };
}
