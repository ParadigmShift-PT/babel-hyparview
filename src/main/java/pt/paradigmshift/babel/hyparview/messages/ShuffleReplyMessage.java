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
 * Reciprocal sample sent by the shuffle's terminating node back to the
 * originator. Carries the same {@code seqnum} as the originating
 * {@link ShuffleMessage} so the originator can correlate the reply with the
 * set of hosts it sent out.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}.
 */
public class ShuffleReplyMessage extends ProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 2508;

    private final short seqnum;
    private final List<Host> sample;

    public ShuffleReplyMessage(Collection<Host> peers, short seqnum) {
        super(MSG_CODE);
        this.sample = new ArrayList<>(peers);
        this.seqnum = seqnum;
    }

    /**
     * @return a <em>defensive copy</em> of the sample. The upstream version
     *         returned the internal list directly, which the receiving
     *         handler then mutated — a bug under any replay / log path.
     */
    public List<Host> getSample() {
        return new ArrayList<>(sample);
    }

    public short getSeqnum() {
        return seqnum;
    }

    @Override
    public String toString() {
        return "ShuffleReplyMessage{seqN=" + seqnum + ", sample=" + sample + '}';
    }

    public static final ISerializer<ShuffleReplyMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(ShuffleReplyMessage msg, ByteBuf out) throws IOException {
            out.writeShort(msg.seqnum);
            out.writeShort(msg.sample.size());
            for (Host h : msg.sample) {
                Host.serializer.serialize(h, out);
            }
        }

        @Override
        public ShuffleReplyMessage deserialize(ByteBuf in) throws IOException {
            short seqnum = in.readShort();
            short size = in.readShort();
            if (size < 0) {
                throw new IOException("ShuffleReplyMessage: negative sample size " + size);
            }
            List<Host> payload = new ArrayList<>(size);
            for (short i = 0; i < size; i++) {
                payload.add(Host.serializer.deserialize(in));
            }
            return new ShuffleReplyMessage(payload, seqnum);
        }
    };
}
