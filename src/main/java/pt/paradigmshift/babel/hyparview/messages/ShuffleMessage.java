package pt.paradigmshift.babel.hyparview.messages;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * Random walk that periodically refreshes the passive view: the originator
 * sends a sample of {@code 1 + kActive + kPassive} hosts (itself plus
 * samples from its active and passive views) to one random active peer,
 * which forwards along until TTL hits zero. The terminating node merges the
 * sample into its passive view and replies with its own sample.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class ShuffleMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 2507;

    private final short seqnum;
    private short ttl;
    private final List<Host> sample;
    private final Host origin;

    public ShuffleMessage(Host self, Collection<Host> peers, short ttl, short seqnum) {
        super(MSG_CODE);
        this.origin = self;
        this.sample = new ArrayList<>(peers);
        this.ttl = ttl;
        this.seqnum = seqnum;
    }

    /**
     * @return the origin plus the originator's sample — used by the
     *         terminating node when computing its passive-view merge
     */
    public List<Host> getFullSample() {
        List<Host> full = new ArrayList<>(sample.size() + 1);
        full.addAll(sample);
        full.add(origin);
        return full;
    }

    public Host getOrigin() {
        return origin;
    }

    public short getTtl() {
        return ttl;
    }

    /** Decrement the TTL; returns the post-decrement value. */
    public short decrementTtl() {
        return --ttl;
    }

    public short getSeqnum() {
        return seqnum;
    }

    @Override
    public String toString() {
        return "ShuffleMessage{origin=" + origin + ", seqN=" + seqnum
                + ", ttl=" + ttl + ", sample=" + sample + '}';
    }

    public static final ISerializer<ShuffleMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ShuffleMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.origin, out);
            out.writeShort(msg.seqnum);
            out.writeShort(msg.ttl);
            out.writeShort(msg.sample.size());
            for (Host h : msg.sample) {
                Host.serializer.serialize(h, out);
            }
        }

        @Override
        public ShuffleMessage deserialize(ByteBuf in) throws IOException {
            Host origin = Host.serializer.deserialize(in);
            short seqnum = in.readShort();
            short ttl = in.readShort();
            short size = in.readShort();
            if (size < 0) {
                throw new IOException("ShuffleMessage: negative sample size " + size);
            }
            List<Host> payload = new ArrayList<>(size);
            for (short i = 0; i < size; i++) {
                payload.add(Host.serializer.deserialize(in));
            }
            return new ShuffleMessage(origin, payload, ttl, seqnum);
        }
    };
}
