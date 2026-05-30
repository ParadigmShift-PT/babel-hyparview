package pt.paradigmshift.babel.hyparview;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pt.paradigmshift.babel.hyparview.messages.DisconnectMessage;
import pt.paradigmshift.babel.hyparview.messages.ForwardJoinMessage;
import pt.paradigmshift.babel.hyparview.messages.JoinMessage;
import pt.paradigmshift.babel.hyparview.messages.JoinReplyMessage;
import pt.paradigmshift.babel.hyparview.messages.NeighborReplyMessage;
import pt.paradigmshift.babel.hyparview.messages.NeighborRequestMessage;
import pt.paradigmshift.babel.hyparview.messages.ShuffleMessage;
import pt.paradigmshift.babel.hyparview.messages.ShuffleReplyMessage;
import pt.paradigmshift.babel.hyparview.timers.CheckConnectivityTimeout;
import pt.paradigmshift.babel.hyparview.timers.ShuffleTimer;
import pt.paradigmshift.babel.hyparview.utils.IView;
import pt.paradigmshift.babel.hyparview.utils.View;
import pt.unl.fct.di.novasys.babel.core.DiscoverableProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.metrics.Counter;
import pt.unl.fct.di.novasys.babel.metrics.Metric;
import pt.unl.fct.di.novasys.babel.protocols.general.notifications.ChannelAvailableNotification;
import pt.unl.fct.di.novasys.babel.protocols.membership.notifications.NeighborDown;
import pt.unl.fct.di.novasys.babel.protocols.membership.notifications.NeighborUp;
import pt.unl.fct.di.novasys.babel.protocols.membership.requests.GetNeighborsSampleReply;
import pt.unl.fct.di.novasys.babel.protocols.membership.requests.GetNeighborsSampleRequest;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * HyParView partial-view membership protocol with bootstrap discovery.
 *
 * <p>Maintains a small fixed-capacity <b>active view</b> (the gossip overlay)
 * and a larger <b>passive view</b> (fallback peers). Joins are propagated by
 * random walks; the passive view is refreshed by periodic shuffles. The
 * {@code with-discovery} variant integrates with Babel-Swarm's
 * {@link DiscoverableProtocol} interface so the protocol can wait for the
 * runtime to push it a contact instead of demanding one in configuration.
 *
 * <p>For the algorithmic background see Leitão et al.,
 * <i>HyParView: a membership protocol for reliable gossip-based broadcast</i>,
 * Proc. DSN 2007.
 *
 * <h2>Threading</h2>
 *
 * All state is touched only from this protocol's Babel event loop. No locking
 * is correct.
 */
public class HyParView extends DiscoverableProtocol {

    private static final Logger logger = LogManager.getLogger(HyParView.class);

    /**
     * Babel protocol numeric identifier.
     *
     * <p>Bumped from the upstream's 1400 to <b>2500</b> to avoid collision
     * with {@code DataProcessingProtocol} (1400) in {@code stoneflux-edgegateway}.
     */
    public static final short PROTOCOL_ID = 2500;
    /** Babel protocol name. */
    public static final String PROTOCOL_NAME = "HyParView";

    /** Maximum exponential backoff for {@link CheckConnectivityTimeout}. */
    private static final int MAX_BACKOFF = 60_000;

    /** Property — active-view capacity (gossip overlay degree). */
    public static final String PAR_ACTIVE_VIEW_SIZE = "HyParView.ActiveView";
    /** Property — passive-view capacity. */
    public static final String PAR_PASSIVE_VIEW_SIZE = "HyParView.PassiveView";
    /** Property — Active Random Walk Length (TTL of the Join walk). */
    public static final String PAR_ARWL = "HyParView.ARWL";
    /** Property — Passive Random Walk Length. */
    public static final String PAR_PRWL = "HyParView.PRWL";
    /** Property — milliseconds between shuffles. */
    public static final String PAR_SHUFFLE_PERIOD = "HyParView.ShufflePeriod";
    /** Property — initial backoff for re-check after active-view drop. */
    public static final String PAR_CHECK_CONNECTIVITY_PERIOD = "HyParView.CheckConnectivityPeriod";
    /** Property — number of active-view peers to include in a shuffle. */
    public static final String PAR_K_A = "HyParView.kActive";
    /** Property — number of passive-view peers to include in a shuffle. */
    public static final String PAR_K_P = "HyParView.kPassive";
    /** Property — optional bootstrap contact {@code host:port}, or {@code none}. */
    public static final String PAR_CONTACT = "HyParView.contact";
    /** Property — TCP bind address. Defaults to the {@code myself} host. */
    public static final String PAR_CHANNEL_ADDRESS = "HyParView.Channel.Address";
    /** Property — TCP bind port. Defaults to the {@code myself} host. */
    public static final String PAR_CHANNEL_PORT = "HyParView.Channel.Port";
    /**
     * Prefix for channel-specific property pass-through. Any property named
     * {@code HyParView.Channel.<key>} (other than {@code Address} / {@code Port})
     * is forwarded to the underlying Babel channel as just {@code <key>}.
     * Used to pipe keystore / truststore / connect-timeout / nonce-size
     * properties through to {@code TLSChannel} or {@code AuthChannel}
     * without HyParView needing to know about them individually.
     */
    public static final String PAR_CHANNEL_PROP_PREFIX = "HyParView.Channel.";

    private final short arwl;
    private final short prwl;
    private final long shufflePeriod;
    private final int originalCheckConnectivityPeriod;
    private int checkConnectivityPeriod;
    private final short kActive;
    private final short kPassive;

    protected final String channelName;
    protected final int channelId;
    protected final Host myself;

    protected final IView active;
    protected final IView passive;

    protected final Set<Host> pending = new HashSet<>();
    private final Map<Short, Host[]> activeShuffles = new HashMap<>();

    private short seqNum = 0;

    protected final Random rnd;

    private boolean isReadyToStart;
    private boolean isStarted;

    private final Counter sentMessagesCounter;

    /**
     * Construct the protocol, parse all properties, create the TCP channel
     * and register message / request / timer / channel-event handlers.
     *
     * @param channelName the channel-name used in the
     *                    {@link ChannelAvailableNotification} fired on
     *                    {@link #start()}; for Babel-Swarm compatibility
     *                    callers usually pass {@code TCPChannel.NAME}
     * @param properties  configuration; see the {@code PAR_*} constants
     * @param myself      this node's own {@link Host} identity
     */
    public HyParView(String channelName, Properties properties, Host myself)
            throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID, myself);
        this.myself = myself;
        this.channelName = channelName;
        this.sentMessagesCounter = registerMetric(
                new Counter.Builder("SentMessages", Metric.Unit.NONE).build());

        int maxActive = Integer.parseInt(properties.getProperty(PAR_ACTIVE_VIEW_SIZE, "4"));
        int maxPassive = Integer.parseInt(properties.getProperty(PAR_PASSIVE_VIEW_SIZE, "7"));
        this.arwl = Short.parseShort(properties.getProperty(PAR_ARWL, "4"));
        this.prwl = Short.parseShort(properties.getProperty(PAR_PRWL, "2"));

        // Time intervals widened from short to long — short overflows at 32768 ms.
        this.shufflePeriod = Long.parseLong(properties.getProperty(PAR_SHUFFLE_PERIOD, "2000"));
        this.originalCheckConnectivityPeriod = Integer.parseInt(
                properties.getProperty(PAR_CHECK_CONNECTIVITY_PERIOD, "1000"));
        this.checkConnectivityPeriod = this.originalCheckConnectivityPeriod;

        this.kActive = Short.parseShort(properties.getProperty(PAR_K_A, "2"));
        this.kPassive = Short.parseShort(properties.getProperty(PAR_K_P, "3"));

        this.rnd = new Random();
        this.active = new View(maxActive, myself, rnd);
        this.passive = new View(maxPassive, myself, rnd);

        this.active.setOther(passive, pending);
        this.passive.setOther(active, pending);

        Properties channelProps = new Properties();
        // Pass through every HyParView.Channel.<key> property to the channel,
        // stripping the prefix. This lets callers configure TLSChannel /
        // AuthChannel properties (keystore, truststore, etc.) via the same
        // namespace as the protocol's own configuration — without HyParView
        // needing to know which channel was selected.
        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith(PAR_CHANNEL_PROP_PREFIX)) {
                String channelKey = name.substring(PAR_CHANNEL_PROP_PREFIX.length());
                channelProps.setProperty(channelKey, properties.getProperty(name));
            }
        }
        // Address/port fall back to myself if not explicitly set above.
        if (!channelProps.containsKey(TCPChannel.ADDRESS_KEY) || !channelProps.containsKey(TCPChannel.PORT_KEY)) {
            if (myself == null) {
                throw new IllegalArgumentException(
                        "Cannot determine bind address: provide " + PAR_CHANNEL_ADDRESS + " + "
                                + PAR_CHANNEL_PORT + ", or pass a non-null myself");
            }
            channelProps.putIfAbsent(TCPChannel.ADDRESS_KEY, myself.getAddress().getHostAddress());
            channelProps.putIfAbsent(TCPChannel.PORT_KEY, Integer.toString(myself.getPort()));
        }
        // The caller chooses the channel type via the channelName constructor
        // argument: TCPChannel.NAME for plain TCP, TLSChannel.NAME for TLS,
        // AuthChannel.NAME for mutual-cert auth. Authentication / OU-check
        // policy is delegated to the channel's TrustManager — see the
        // README's "Cert-based peer authentication" section.
        this.channelId = createChannel(channelName, channelProps);
        setDefaultChannel(channelId);

        registerMessageSerializer(channelId, JoinMessage.MSG_CODE, JoinMessage.serializer);
        registerMessageSerializer(channelId, JoinReplyMessage.MSG_CODE, JoinReplyMessage.serializer);
        registerMessageSerializer(channelId, ForwardJoinMessage.MSG_CODE, ForwardJoinMessage.serializer);
        registerMessageSerializer(channelId, NeighborRequestMessage.MSG_CODE, NeighborRequestMessage.serializer);
        registerMessageSerializer(channelId, NeighborReplyMessage.MSG_CODE, NeighborReplyMessage.serializer);
        registerMessageSerializer(channelId, DisconnectMessage.MSG_CODE, DisconnectMessage.serializer);
        registerMessageSerializer(channelId, ShuffleMessage.MSG_CODE, ShuffleMessage.serializer);
        registerMessageSerializer(channelId, ShuffleReplyMessage.MSG_CODE, ShuffleReplyMessage.serializer);

        registerMessageHandler(channelId, JoinMessage.MSG_CODE, this::uponReceiveJoin);
        registerMessageHandler(channelId, JoinReplyMessage.MSG_CODE, this::uponReceiveJoinReply);
        registerMessageHandler(channelId, ForwardJoinMessage.MSG_CODE, this::uponReceiveForwardJoin);
        registerMessageHandler(channelId, NeighborRequestMessage.MSG_CODE, this::uponReceiveNeighborRequest);
        registerMessageHandler(channelId, NeighborReplyMessage.MSG_CODE, this::uponReceiveNeighborReply);
        registerMessageHandler(channelId, DisconnectMessage.MSG_CODE, this::uponReceiveDisconnect,
                this::uponDisconnectSent);
        registerMessageHandler(channelId, ShuffleMessage.MSG_CODE, this::uponShuffle);
        registerMessageHandler(channelId, ShuffleReplyMessage.MSG_CODE, this::uponReceiveShuffleReply,
                this::uponShuffleReplySent);

        registerRequestHandler(GetNeighborsSampleRequest.REQUEST_ID, this::uponGetSampleRequest);

        registerTimerHandler(ShuffleTimer.TIMER_CODE, this::uponShuffleTimer);
        registerTimerHandler(CheckConnectivityTimeout.TIMER_CODE, this::uponCheckConnectivityTimer);

        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        String contact = props.getProperty(PAR_CONTACT);
        if (contact == null || contact.isBlank() || contact.trim().equalsIgnoreCase("none")) {
            logger.debug("No contact configured — waiting on discovery to start");
            this.isReadyToStart = (contact != null); // explicit "none" = first node, ready
        } else {
            try {
                String[] elems = contact.trim().split(":");
                Host c = new Host(InetAddress.getByName(elems[0]), Integer.parseInt(elems[1]));
                this.addContact(c);
            } catch (Exception e) {
                throw new IOException("Invalid HyParView.contact: " + contact, e);
            }
        }
        this.isStarted = false;
        if (this.isReadyToStart) {
            start();
        }
    }

    /* ───────────────────────── Request handlers ──────────────────────── */

    private void uponGetSampleRequest(GetNeighborsSampleRequest req, short protoID) {
        Set<Host> peers = active.getPeers();
        if (req.getSampleSize() >= peers.size()) {
            sendReply(new GetNeighborsSampleReply(new HashSet<>(peers)), protoID);
            return;
        }
        // Fisher-Yates partial-shuffle sampling using the protocol's own
        // Random — avoids the upstream's per-call SecureRandom allocation
        // and O(n^2) repeated-remove loop.
        List<Host> list = new ArrayList<>(peers);
        int target = req.getSampleSize();
        for (int i = 0; i < target; i++) {
            int j = i + rnd.nextInt(list.size() - i);
            Collections.swap(list, i, j);
        }
        sendReply(new GetNeighborsSampleReply(new HashSet<>(list.subList(0, target))), protoID);
    }

    /* ───────────────────────── Message handlers ──────────────────────── */

    private void uponReceiveJoin(JoinMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        Host evicted = active.addPeer(from);
        logger.trace("After add: active{}", active);
        openConnection(from);
        triggerNotification(new NeighborUp(from));
        sendMessage(new JoinReplyMessage(), from);
        sentMessagesCounter.inc();

        for (Host peer : active.getPeers()) {
            if (!peer.equals(from)) {
                sendMessage(new ForwardJoinMessage(arwl, from), peer);
                sentMessagesCounter.inc();
            }
        }
        handleDropFromActive(evicted);
    }

    private void uponReceiveJoinReply(JoinReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        if (active.containsPeer(from)) {
            return;
        }
        passive.removePeer(from);
        pending.remove(from);
        Host evicted = active.addPeer(from);
        openConnection(from);
        triggerNotification(new NeighborUp(from));
        handleDropFromActive(evicted);
    }

    private void uponReceiveForwardJoin(ForwardJoinMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        // Per upstream semantics: decrementTtl() returns pre-decrement value,
        // so the walk terminates when the receiver sees ttl == 0.
        if (msg.decrementTtl() == 0 || active.getPeers().size() == 1) {
            Host newHost = msg.getNewHost();
            if (!newHost.equals(myself) && !active.containsPeer(newHost)) {
                passive.removePeer(newHost);
                pending.remove(newHost);
                Host evicted = active.addPeer(newHost);
                openConnection(newHost);
                triggerNotification(new NeighborUp(newHost));
                sendMessage(new JoinReplyMessage(), newHost);
                sentMessagesCounter.inc();
                handleDropFromActive(evicted);
            }
            return;
        }

        if (msg.getTtl() == prwl) {
            // PRWL hop — also remember the joiner in passive
            passive.addPeer(msg.getNewHost());
        }
        Host next = active.getRandomDiff(from, msg.getNewHost());
        if (next != null) {
            sendMessage(msg, next);
            sentMessagesCounter.inc();
        }
    }

    private void uponReceiveNeighborRequest(NeighborRequestMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        if (msg.isPriority()) {
            // High priority: always accept.
            if (!active.containsPeer(from)) {
                pending.remove(from);
                passive.removePeer(from);
                Host evicted = active.addPeer(from);
                openConnection(from);
                triggerNotification(new NeighborUp(from));
                handleDropFromActive(evicted);
            }
            sendMessage(new NeighborReplyMessage(true), from);
            sentMessagesCounter.inc();
            return;
        }

        // Low priority: accept if room (counting pending), refuse otherwise.
        pending.remove(from);
        if (!active.fullWithPending(pending) || active.containsPeer(from)) {
            if (!active.containsPeer(from)) {
                passive.removePeer(from);
                active.addPeer(from);
                openConnection(from);
                triggerNotification(new NeighborUp(from));
            }
            sendMessage(new NeighborReplyMessage(true), from);
            sentMessagesCounter.inc();
        } else {
            sendMessage(new NeighborReplyMessage(false), from, TCPChannel.CONNECTION_IN);
            sentMessagesCounter.inc();
        }
    }

    private void uponReceiveNeighborReply(NeighborReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        pending.remove(from);
        if (msg.isAccepted()) {
            if (!active.containsPeer(from)) {
                checkConnectivityPeriod = originalCheckConnectivityPeriod;
                Host evicted = active.addPeer(from);
                openConnection(from);
                triggerNotification(new NeighborUp(from));
                handleDropFromActive(evicted);
            }
        } else if (!active.containsPeer(from)) {
            passive.addPeer(from);
            closeConnection(from);
            if (!active.fullWithPending(pending)) {
                setupTimer(new CheckConnectivityTimeout(), checkConnectivityPeriod);
            }
        }
    }

    private void uponReceiveDisconnect(DisconnectMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        if (!active.containsPeer(from)) {
            return;
        }
        active.removePeer(from);
        handleDropFromActive(from);

        if (active.getPeers().isEmpty()) {
            checkConnectivityPeriod = originalCheckConnectivityPeriod;
        }
        if (!active.fullWithPending(pending)) {
            setupTimer(new CheckConnectivityTimeout(), checkConnectivityPeriod);
        }
    }

    private void uponDisconnectSent(DisconnectMessage msg, Host host, short destProto, int channelId) {
        logger.trace("Sent {} to {}", msg, host);
        closeConnection(host);
    }

    private void uponShuffle(ShuffleMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        // Decrement and forward if still alive.
        if (msg.decrementTtl() > 0) {
            Host next = active.getRandomDiff(from, msg.getOrigin());
            if (next != null) {
                sendMessage(msg, next);
                sentMessagesCounter.inc();
                return;
            }
        }

        // Walk terminates here. Build our reply sample, send back to origin,
        // and merge the originator's sample into our passive view.
        Set<Host> ourSample = new HashSet<>(passive.getRandomSample(1 + kActive + kPassive));
        Host[] ourSampleArr = ourSample.toArray(new Host[0]);

        if (!active.containsPeer(msg.getOrigin()) && !pending.contains(msg.getOrigin())) {
            openConnection(msg.getOrigin());
        }
        sendMessage(new ShuffleReplyMessage(ourSample, msg.getSeqnum()), msg.getOrigin());
        sentMessagesCounter.inc();

        mergeShuffleSampleIntoPassive(msg.getFullSample(), ourSampleArr);
    }

    private void uponShuffleReplySent(ShuffleReplyMessage msg, Host host, short destProto, int channelId) {
        if (!active.containsPeer(host) && !pending.contains(host)) {
            logger.trace("Disconnecting from {} after shuffleReply", host);
            closeConnection(host);
        }
    }

    private void uponReceiveShuffleReply(ShuffleReplyMessage msg, Host from, short sourceProto, int channelId) {
        logger.debug("Received {} from {}", msg, from);
        Host[] sent = activeShuffles.remove(msg.getSeqnum());
        if (sent == null) sent = new Host[0];

        // Defensive copy — upstream returned the message's internal list and
        // mutated it during the merge, corrupting the message for any later
        // logging / replay path.
        List<Host> incoming = new ArrayList<>(msg.getSample());
        mergeShuffleSampleIntoPassive(incoming, sent);
    }

    /* ────────────────────────────── Timers ───────────────────────────── */

    private void uponShuffleTimer(ShuffleTimer timer, long timerId) {
        if (!active.fullWithPending(pending)) {
            setupTimer(new CheckConnectivityTimeout(), checkConnectivityPeriod);
        }

        Host target = active.getRandom();
        if (target == null) return;

        Set<Host> sample = new HashSet<>();
        sample.addAll(active.getRandomSample(kActive));
        sample.addAll(passive.getRandomSample(kPassive));
        activeShuffles.put(seqNum, sample.toArray(new Host[0]));
        sendMessage(new ShuffleMessage(myself, sample, arwl, seqNum), target);
        sentMessagesCounter.inc();
        seqNum = (short) ((seqNum + 1) & 0x7FFF); // wrap to [0, Short.MAX_VALUE]
    }

    private void uponCheckConnectivityTimer(CheckConnectivityTimeout timer, long timerId) {
        if (active.fullWithPending(pending)) {
            return;
        }
        Host candidate = passive.dropRandom();
        if (candidate == null) {
            return;
        }
        if (pending.add(candidate)) {
            logger.trace("Promoting from passive: {} pending={} active={} passive={}",
                         candidate, pending, active, passive);
            openConnection(candidate);
            sendMessage(new NeighborRequestMessage(getPriority()), candidate);
            sentMessagesCounter.inc();
            // Double the backoff up to the 60 s cap. Widened to int — the upstream
            // performed this in short space, so backoffs above 16 s wrapped negative.
            checkConnectivityPeriod = Math.min(checkConnectivityPeriod * 2, MAX_BACKOFF);
        } else {
            // Already pending; put it back into passive.
            passive.addPeer(candidate);
        }
    }

    private boolean getPriority() {
        return active.getPeers().size() + pending.size() == 1;
    }

    /* ───────────────────────── Channel events ─────────────────────────── */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host h = event.getNode();
        logger.info("Out-connection to {} is down: {}", h, event.getCause());
        if (active.removePeer(h)) {
            triggerNotification(new NeighborDown(h, true));
            if (!active.fullWithPending(pending)) {
                setupTimer(new CheckConnectivityTimeout(), checkConnectivityPeriod);
            }
        }
        pending.remove(h);
    }

    private void uponOutConnectionFailed(OutConnectionFailed<?> event, int channelId) {
        Host h = event.getNode();
        logger.info("Out-connection to {} failed: {}", h, event.getCause());
        if (active.removePeer(h)) {
            triggerNotification(new NeighborDown(h, true));
            if (!active.fullWithPending(pending)) {
                setupTimer(new CheckConnectivityTimeout(), checkConnectivityPeriod);
            }
        }
        pending.remove(h);
        passive.removePeer(h);
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.trace("Out-connection to {} up", event.getNode());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("In-connection from {} up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("In-connection from {} down: {}", event.getNode(), event.getCause());
    }

    /* ─────────────────────── DiscoverableProtocol ────────────────────── */

    @Override
    public void start() {
        if (this.isStarted) return;
        triggerNotification(new ChannelAvailableNotification(
                PROTOCOL_ID, PROTOCOL_NAME, this.channelId, channelName, myself));
        setupPeriodicTimer(new ShuffleTimer(), this.shufflePeriod, this.shufflePeriod);
        this.isStarted = true;
    }

    @Override
    public boolean readyToStart() {
        logger.debug("{}: ready to start = {}", PROTOCOL_NAME, isReadyToStart);
        return this.isReadyToStart;
    }

    @Override
    public boolean needsDiscovery() {
        return !this.isReadyToStart;
    }

    @Override
    public void addContact(Host host) {
        openConnection(host);
        sendMessage(new JoinMessage(), host);
        sentMessagesCounter.inc();
        this.isReadyToStart = true;
        logger.info("Sent JoinMessage to {}", host);
    }

    @Override
    public Host getContact() {
        return this.active.getRandom();
    }

    /* ────────────────────────── Public access ────────────────────────── */

    public int getChannel() {
        return channelId;
    }

    /* ────────────────────────────── Helpers ──────────────────────────── */

    /**
     * Called when {@link IView#addPeer(Host)} returns an evicted peer: notify
     * downstream observers, tell the evicted peer we've dropped it, and move
     * it to the passive view.
     */
    protected void handleDropFromActive(Host dropped) {
        if (dropped == null) return;
        triggerNotification(new NeighborDown(dropped, false));
        sendMessage(new DisconnectMessage(), dropped);
        sentMessagesCounter.inc();
        passive.addPeer(dropped);
        logger.trace("Moved {} from active to passive: passive{}", dropped, passive);
    }

    /**
     * Merge an incoming shuffle sample into the local passive view. Drops
     * hosts that are already known (active / passive / self) from the
     * incoming set, evicts hosts we recently sent out (so symmetric shuffles
     * actually exchange peers), and finally adds what fits.
     *
     * @param incoming hosts the remote shared with us (mutated in place;
     *                 caller must pass a defensive copy)
     * @param sentOut  hosts we sent out in the originating shuffle (used as
     *                 priority eviction candidates)
     */
    private void mergeShuffleSampleIntoPassive(List<Host> incoming, Host[] sentOut) {
        // Filter out already-known hosts (and ourselves).
        incoming.removeIf(h -> h.equals(myself) || active.containsPeer(h) || passive.containsPeer(h));

        // Evict our recently-sent-out hosts first to make room.
        int i = 0;
        while (i < sentOut.length
                && passive.getPeers().size() + incoming.size() > passive.getCapacity()) {
            passive.removePeer(sentOut[i]);
            i++;
        }
        // Then random-drop until we fit.
        while (passive.getPeers().size() + incoming.size() > passive.getCapacity()) {
            passive.dropRandom();
        }
        for (Host h : incoming) {
            passive.addPeer(h);
        }
        logger.trace("After merge: passive{}", passive);
    }
}
