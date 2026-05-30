package pt.paradigmshift.babel.hyparview.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * Random walk message that propagates a Join through the overlay: carries the
 * joiner's {@link Host} and a TTL. Each hop decrements the TTL; when it
 * reaches zero (or the recipient has only one active peer) the receiver
 * accepts the joiner into its active view and sends a {@code JoinReplyMessage}
 * back.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class ForwardJoinMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 2503;

    private short ttl;
    private final Host newHost;

    public ForwardJoinMessage(short ttl, Host newHost) {
        super(MSG_CODE);
        this.ttl = ttl;
        this.newHost = newHost;
    }

    public Host getNewHost() {
        return newHost;
    }

    public short getTtl() {
        return ttl;
    }

    /**
     * Decrement the TTL and return the <em>pre-decrement</em> value so the
     * caller can compare against zero. Preserves upstream walk semantics: a
     * message constructed with {@code ttl=ARWL} fires the local Join action
     * after {@code ARWL+1} hops along the walk.
     */
    public short decrementTtl() {
        return ttl--;
    }

    @Override
    public String toString() {
        return "ForwardJoinMessage{ttl=" + ttl + ", newHost=" + newHost + '}';
    }

    public static final ISerializer<ForwardJoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ForwardJoinMessage m, ByteBuf out) throws IOException {
            out.writeShort(m.ttl);
            Host.serializer.serialize(m.newHost, out);
        }

        @Override
        public ForwardJoinMessage deserialize(ByteBuf in) throws IOException {
            short ttl = in.readShort();
            Host newHost = Host.serializer.deserialize(in);
            return new ForwardJoinMessage(ttl, newHost);
        }
    };
}
