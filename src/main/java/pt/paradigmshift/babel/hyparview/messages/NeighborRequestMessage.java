package pt.paradigmshift.babel.hyparview.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/**
 * "Will you promote me into your active view?" message — sent by a node
 * whose own active view is below capacity. The {@code priority} flag is
 * {@code true} when the requester has no active peers at all
 * (high-priority — recipient should always accept) and {@code false}
 * otherwise (low-priority — recipient may refuse if its own active view is
 * full).
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class NeighborRequestMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 2505;

    private final boolean priority;

    public NeighborRequestMessage(boolean priority) {
        super(MSG_CODE);
        this.priority = priority;
    }

    public boolean isPriority() {
        return priority;
    }

    @Override
    public String toString() {
        return "NeighborRequestMessage{priority=" + priority + '}';
    }

    public static final ISerializer<NeighborRequestMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(NeighborRequestMessage m, ByteBuf out) {
            out.writeBoolean(m.priority);
        }

        @Override
        public NeighborRequestMessage deserialize(ByteBuf in) {
            return new NeighborRequestMessage(in.readBoolean());
        }
    };
}
