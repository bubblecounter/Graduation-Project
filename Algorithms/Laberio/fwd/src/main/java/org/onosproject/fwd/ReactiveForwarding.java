/*
 * Copyright 2014-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.fwd;
import com.google.common.collect.ImmutableSet;
import org.onlab.packet.Ethernet;
import org.onlab.packet.ICMP;
import org.onlab.packet.ICMP6;
import org.onlab.packet.IPv4;
import org.onlab.packet.IPv6;
import org.onlab.packet.Ip4Prefix;
import org.onlab.packet.Ip6Prefix;
import org.onlab.packet.MacAddress;
import org.onlab.packet.TCP;
import org.onlab.packet.TpPort;
import org.onlab.packet.UDP;
import org.onlab.packet.VlanId;
import org.onlab.util.KryoNamespace;
import org.onlab.util.Tools;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.event.Event;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.HostId;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowEntry;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.EthCriterion;
import org.onosproject.net.flow.instructions.Instruction;
import org.onosproject.net.flow.instructions.Instructions;
import org.onosproject.net.flowobjective.DefaultForwardingObjective;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.ForwardingObjective;
import org.onosproject.net.host.HostService;
import org.onosproject.net.link.LinkEvent;
import org.onosproject.net.link.LinkService;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketPriority;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.topology.TopologyEvent;
import org.onosproject.net.topology.TopologyListener;
import org.onosproject.net.topology.TopologyService;
import org.onosproject.store.serializers.KryoNamespaces;
import org.onosproject.store.service.EventuallyConsistentMap;
import org.onosproject.store.service.MultiValuedTimestamp;
import org.onosproject.store.service.StorageService;
import org.onosproject.store.service.WallClockTimestamp;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Modified;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.stream.Collectors.toList;
import static org.onlab.util.Tools.groupedThreads;
import static org.onosproject.fwd.OsgiPropertyConstants.FLOW_PRIORITY;
import static org.onosproject.fwd.OsgiPropertyConstants.FLOW_PRIORITY_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.FLOW_TIMEOUT;
import static org.onosproject.fwd.OsgiPropertyConstants.FLOW_TIMEOUT_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.IGNORE_IPV4_MCAST_PACKETS;
import static org.onosproject.fwd.OsgiPropertyConstants.IGNORE_IPV4_MCAST_PACKETS_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.IPV6_FORWARDING;
import static org.onosproject.fwd.OsgiPropertyConstants.IPV6_FORWARDING_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_DST_MAC_ONLY;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_DST_MAC_ONLY_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_ICMP_FIELDS;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_ICMP_FIELDS_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_IPV4_ADDRESS;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_IPV4_ADDRESS_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_IPV4_DSCP;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_IPV4_DSCP_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_IPV6_ADDRESS;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_IPV6_ADDRESS_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_IPV6_FLOW_LABEL;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_IPV6_FLOW_LABEL_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_TCP_UDP_PORTS;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_TCP_UDP_PORTS_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_VLAN_ID;
import static org.onosproject.fwd.OsgiPropertyConstants.MATCH_VLAN_ID_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.PACKET_OUT_OFPP_TABLE;
import static org.onosproject.fwd.OsgiPropertyConstants.PACKET_OUT_OFPP_TABLE_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.PACKET_OUT_ONLY;
import static org.onosproject.fwd.OsgiPropertyConstants.PACKET_OUT_ONLY_DEFAULT;
import static org.onosproject.fwd.OsgiPropertyConstants.RECORD_METRICS;
import static org.onosproject.fwd.OsgiPropertyConstants.RECORD_METRICS_DEFAULT;
import static org.onosproject.net.flow.criteria.Criterion.Type.ETH_DST;
import static org.onosproject.net.flow.criteria.Criterion.Type.ETH_SRC;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Sample reactive forwarding application.
 */
@Component(
        immediate = true,
        service = ReactiveForwarding.class,
        property = {
                PACKET_OUT_ONLY + ":Boolean=" + PACKET_OUT_ONLY_DEFAULT,
                PACKET_OUT_OFPP_TABLE + ":Boolean=" + PACKET_OUT_OFPP_TABLE_DEFAULT,
                FLOW_TIMEOUT + ":Integer=" + FLOW_TIMEOUT_DEFAULT,
                FLOW_PRIORITY  + ":Integer=" + FLOW_PRIORITY_DEFAULT,
                IPV6_FORWARDING + ":Boolean=" + IPV6_FORWARDING_DEFAULT,
                MATCH_DST_MAC_ONLY + ":Boolean=" + MATCH_DST_MAC_ONLY_DEFAULT,
                MATCH_VLAN_ID + ":Boolean=" + MATCH_VLAN_ID_DEFAULT,
                MATCH_IPV4_ADDRESS + ":Boolean=" + MATCH_IPV4_ADDRESS_DEFAULT,
                MATCH_IPV4_DSCP + ":Boolean=" + MATCH_IPV4_DSCP_DEFAULT,
                MATCH_IPV6_ADDRESS + ":Boolean=" + MATCH_IPV6_ADDRESS_DEFAULT,
                MATCH_IPV6_FLOW_LABEL + ":Boolean=" + MATCH_IPV6_FLOW_LABEL_DEFAULT,
                MATCH_TCP_UDP_PORTS + ":Boolean=" + MATCH_TCP_UDP_PORTS_DEFAULT,
                MATCH_ICMP_FIELDS + ":Boolean=" + MATCH_ICMP_FIELDS_DEFAULT,
                IGNORE_IPV4_MCAST_PACKETS + ":Boolean=" + IGNORE_IPV4_MCAST_PACKETS_DEFAULT,
                RECORD_METRICS + ":Boolean=" + RECORD_METRICS_DEFAULT
        }
)
public class ReactiveForwarding {

    private final Logger log = getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected TopologyService topologyService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected PacketService packetService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected HostService hostService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowRuleService flowRuleService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowObjectiveService flowObjectiveService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ComponentConfigService cfgService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected StorageService storageService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected DeviceService deviceService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected LinkService linkService;


    private ReactivePacketProcessor processor = new ReactivePacketProcessor();

    private  EventuallyConsistentMap<MacAddress, ReactiveForwardMetrics> metrics;

    private final executionSynchronisation runLoadBalance = new executionSynchronisation();

    private ApplicationId appId;

    private int DEFAULT_RULE_PRIO = 50000;
    private final int LOCAL_RULE_PRIO = 60000;
    private final int DEFAULT_RULE_TIMEOUT = 60;
    private final int LOCAL_RULE_TIMEOUT = 600;
    private int LINK_CAPACITY = 20000000;
    private int FLOW_COUNT = 0;
    private final int PATH_REFRESH_TIME = 10;

    private final int test_scenario = 4;

    /** Enable packet-out only forwarding; default is false. */
    private boolean packetOutOnly = PACKET_OUT_ONLY_DEFAULT;

    /** Enable first packet forwarding using OFPP_TABLE port instead of PacketOut with actual port; default is false. */
    private boolean packetOutOfppTable = PACKET_OUT_OFPP_TABLE_DEFAULT;

    /** Configure Flow Timeout for installed flow rules; default is 10 sec. */
    private int flowTimeout = FLOW_TIMEOUT_DEFAULT;

    /** Configure Flow Priority for installed flow rules; default is 10. */
    private int flowPriority = FLOW_PRIORITY_DEFAULT;

    /** Enable IPv6 forwarding; default is false. */
    private boolean ipv6Forwarding = IPV6_FORWARDING_DEFAULT;

    /** Enable matching Dst Mac Only; default is false. */
    private boolean matchDstMacOnly = MATCH_DST_MAC_ONLY_DEFAULT;

    /** Enable matching Vlan ID; default is false. */
    private boolean matchVlanId = MATCH_VLAN_ID_DEFAULT;

    /** Enable matching IPv4 Addresses; default is false. */
    private boolean matchIpv4Address = MATCH_IPV4_ADDRESS_DEFAULT;

    /** Enable matching IPv4 DSCP and ECN; default is false. */
    private boolean matchIpv4Dscp = MATCH_IPV4_DSCP_DEFAULT;

    /** Enable matching IPv6 Addresses; default is false. */
    private boolean matchIpv6Address = MATCH_IPV6_ADDRESS_DEFAULT;

    /** Enable matching IPv6 FlowLabel; default is false. */
    private boolean matchIpv6FlowLabel = MATCH_IPV6_FLOW_LABEL_DEFAULT;

    /** Enable matching TCP/UDP ports; default is false. */
    private boolean matchTcpUdpPorts = MATCH_TCP_UDP_PORTS_DEFAULT;

    /** Enable matching ICMPv4 and ICMPv6 fields; default is false. */
    private boolean matchIcmpFields = MATCH_ICMP_FIELDS_DEFAULT;

    /** Ignore (do not forward) IPv4 multicast packets; default is false. */
    private boolean ignoreIPv4Multicast = IGNORE_IPV4_MCAST_PACKETS_DEFAULT;

    /** Enable record metrics for reactive forwarding. */
    private boolean recordMetrics = RECORD_METRICS_DEFAULT;

    private final TopologyListener topologyListener = new InternalTopologyListener();

    private ExecutorService blackHoleExecutor;
    private boolean disable_service = false;

    Thread t1 = new Thread(new Runnable() {
        @Override
        public void run() {
            log.info("### Starting LABERIO THREAD");
            while(!disable_service){

                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                laberio();
            }

        }
    });

    @Activate
    public void activate(ComponentContext context) {
        KryoNamespace.Builder metricSerializer = KryoNamespace.newBuilder()
                .register(KryoNamespaces.API)
                .register(ReactiveForwardMetrics.class)
                .register(MultiValuedTimestamp.class);
        metrics =  storageService.<MacAddress, ReactiveForwardMetrics>eventuallyConsistentMapBuilder()
                .withName("metrics-fwd")
                .withSerializer(metricSerializer)
                .withTimestampProvider((key, metricsData) -> new
                        MultiValuedTimestamp<>(new WallClockTimestamp(), System.nanoTime()))
                .build();
        disable_service = false;
        blackHoleExecutor = newSingleThreadExecutor(groupedThreads("onos/app/fwd",
                                                                   "black-hole-fixer",
                                                                   log));

        cfgService.registerProperties(getClass());
        cfgService.setProperty("org.onosproject.provider.of.device.impl.OpenFlowDeviceProvider", "portStatsPollFrequency", "1");

        appId = coreService.registerApplication("org.onosproject.fwd");

        packetService.addProcessor(processor, PacketProcessor.director(2));
        topologyService.addListener(topologyListener);
        readComponentConfiguration(context);
        requestIntercepts();

        t1.start();
        createLocalRules();
        log.info("Started", appId.id());
    }

    @Deactivate
    public void deactivate() {
        cfgService.unregisterProperties(getClass(), false);
        withdrawIntercepts();
        flowRuleService.removeFlowRulesById(appId);
        packetService.removeProcessor(processor);
        topologyService.removeListener(topologyListener);
        blackHoleExecutor.shutdown();
        blackHoleExecutor = null;
        processor = null;
        disable_service = true;
        log.info("Stopped");
    }

    @Modified
    public void modified(ComponentContext context) {
        readComponentConfiguration(context);
        requestIntercepts();
    }

    /**
     * Request packet in via packet service.
     */
    private void requestIntercepts() {
        TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
        selector.matchEthType(Ethernet.TYPE_IPV4);
        packetService.requestPackets(selector.build(), PacketPriority.REACTIVE, appId);

        selector.matchEthType(Ethernet.TYPE_IPV6);
        if (ipv6Forwarding) {
            packetService.requestPackets(selector.build(), PacketPriority.REACTIVE, appId);
        } else {
            packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId);
        }
    }

    /**
     * Cancel request for packet in via packet service.
     */
    private void withdrawIntercepts() {
        TrafficSelector.Builder selector = DefaultTrafficSelector.builder();
        selector.matchEthType(Ethernet.TYPE_IPV4);
        packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId);
        selector.matchEthType(Ethernet.TYPE_IPV6);
        packetService.cancelPackets(selector.build(), PacketPriority.REACTIVE, appId);
    }

    /**
     * Extracts properties from the component configuration context.
     *
     * @param context the component context
     */
    private void readComponentConfiguration(ComponentContext context) {
        Dictionary<?, ?> properties = context.getProperties();

        Boolean packetOutOnlyEnabled =
                Tools.isPropertyEnabled(properties, PACKET_OUT_ONLY);
        if (packetOutOnlyEnabled == null) {
            log.info("Packet-out is not configured, " +
                             "using current value of {}", packetOutOnly);
        } else {
            packetOutOnly = packetOutOnlyEnabled;
            log.info("Configured. Packet-out only forwarding is {}",
                     packetOutOnly ? "enabled" : "disabled");
        }

        Boolean packetOutOfppTableEnabled =
                Tools.isPropertyEnabled(properties, PACKET_OUT_OFPP_TABLE);
        if (packetOutOfppTableEnabled == null) {
            log.info("OFPP_TABLE port is not configured, " +
                             "using current value of {}", packetOutOfppTable);
        } else {
            packetOutOfppTable = packetOutOfppTableEnabled;
            log.info("Configured. Forwarding using OFPP_TABLE port is {}",
                     packetOutOfppTable ? "enabled" : "disabled");
        }

        Boolean ipv6ForwardingEnabled =
                Tools.isPropertyEnabled(properties, IPV6_FORWARDING);
        if (ipv6ForwardingEnabled == null) {
            log.info("IPv6 forwarding is not configured, " +
                             "using current value of {}", ipv6Forwarding);
        } else {
            ipv6Forwarding = ipv6ForwardingEnabled;
            log.info("Configured. IPv6 forwarding is {}",
                     ipv6Forwarding ? "enabled" : "disabled");
        }

        Boolean matchDstMacOnlyEnabled =
                Tools.isPropertyEnabled(properties, MATCH_DST_MAC_ONLY);
        if (matchDstMacOnlyEnabled == null) {
            log.info("Match Dst MAC is not configured, " +
                             "using current value of {}", matchDstMacOnly);
        } else {
            matchDstMacOnly = matchDstMacOnlyEnabled;
            log.info("Configured. Match Dst MAC Only is {}",
                     matchDstMacOnly ? "enabled" : "disabled");
        }

        Boolean matchVlanIdEnabled =
                Tools.isPropertyEnabled(properties, MATCH_VLAN_ID);
        if (matchVlanIdEnabled == null) {
            log.info("Matching Vlan ID is not configured, " +
                             "using current value of {}", matchVlanId);
        } else {
            matchVlanId = matchVlanIdEnabled;
            log.info("Configured. Matching Vlan ID is {}",
                     matchVlanId ? "enabled" : "disabled");
        }

        Boolean matchIpv4AddressEnabled =
                Tools.isPropertyEnabled(properties, MATCH_IPV4_ADDRESS);
        if (matchIpv4AddressEnabled == null) {
            log.info("Matching IPv4 Address is not configured, " +
                             "using current value of {}", matchIpv4Address);
        } else {
            matchIpv4Address = matchIpv4AddressEnabled;
            log.info("Configured. Matching IPv4 Addresses is {}",
                     matchIpv4Address ? "enabled" : "disabled");
        }

        Boolean matchIpv4DscpEnabled =
                Tools.isPropertyEnabled(properties, MATCH_IPV4_DSCP);
        if (matchIpv4DscpEnabled == null) {
            log.info("Matching IPv4 DSCP and ECN is not configured, " +
                             "using current value of {}", matchIpv4Dscp);
        } else {
            matchIpv4Dscp = matchIpv4DscpEnabled;
            log.info("Configured. Matching IPv4 DSCP and ECN is {}",
                     matchIpv4Dscp ? "enabled" : "disabled");
        }

        Boolean matchIpv6AddressEnabled =
                Tools.isPropertyEnabled(properties, MATCH_IPV6_ADDRESS);
        if (matchIpv6AddressEnabled == null) {
            log.info("Matching IPv6 Address is not configured, " +
                             "using current value of {}", matchIpv6Address);
        } else {
            matchIpv6Address = matchIpv6AddressEnabled;
            log.info("Configured. Matching IPv6 Addresses is {}",
                     matchIpv6Address ? "enabled" : "disabled");
        }

        Boolean matchIpv6FlowLabelEnabled =
                Tools.isPropertyEnabled(properties, MATCH_IPV6_FLOW_LABEL);
        if (matchIpv6FlowLabelEnabled == null) {
            log.info("Matching IPv6 FlowLabel is not configured, " +
                             "using current value of {}", matchIpv6FlowLabel);
        } else {
            matchIpv6FlowLabel = matchIpv6FlowLabelEnabled;
            log.info("Configured. Matching IPv6 FlowLabel is {}",
                     matchIpv6FlowLabel ? "enabled" : "disabled");
        }

        Boolean matchTcpUdpPortsEnabled =
                Tools.isPropertyEnabled(properties, MATCH_TCP_UDP_PORTS);
        if (matchTcpUdpPortsEnabled == null) {
            log.info("Matching TCP/UDP fields is not configured, " +
                             "using current value of {}", matchTcpUdpPorts);
        } else {
            matchTcpUdpPorts = matchTcpUdpPortsEnabled;
            log.info("Configured. Matching TCP/UDP fields is {}",
                     matchTcpUdpPorts ? "enabled" : "disabled");
        }

        Boolean matchIcmpFieldsEnabled =
                Tools.isPropertyEnabled(properties, MATCH_ICMP_FIELDS);
        if (matchIcmpFieldsEnabled == null) {
            log.info("Matching ICMP (v4 and v6) fields is not configured, " +
                             "using current value of {}", matchIcmpFields);
        } else {
            matchIcmpFields = matchIcmpFieldsEnabled;
            log.info("Configured. Matching ICMP (v4 and v6) fields is {}",
                     matchIcmpFields ? "enabled" : "disabled");
        }

        Boolean ignoreIpv4McastPacketsEnabled =
                Tools.isPropertyEnabled(properties, IGNORE_IPV4_MCAST_PACKETS);
        if (ignoreIpv4McastPacketsEnabled == null) {
            log.info("Ignore IPv4 multi-cast packet is not configured, " +
                             "using current value of {}", ignoreIPv4Multicast);
        } else {
            ignoreIPv4Multicast = ignoreIpv4McastPacketsEnabled;
            log.info("Configured. Ignore IPv4 multicast packets is {}",
                     ignoreIPv4Multicast ? "enabled" : "disabled");
        }
        Boolean recordMetricsEnabled =
                Tools.isPropertyEnabled(properties, RECORD_METRICS);
        if (recordMetricsEnabled == null) {
            log.info("IConfigured. Ignore record metrics  is {} ," +
                             "using current value of {}", recordMetrics);
        } else {
            recordMetrics = recordMetricsEnabled;
            log.info("Configured. record metrics  is {}",
                     recordMetrics ? "enabled" : "disabled");
        }

        flowTimeout = Tools.getIntegerProperty(properties, FLOW_TIMEOUT, FLOW_TIMEOUT_DEFAULT);
        log.info("Configured. Flow Timeout is configured to {} seconds", flowTimeout);

        flowPriority = Tools.getIntegerProperty(properties, FLOW_PRIORITY, FLOW_PRIORITY_DEFAULT);
        log.info("Configured. Flow Priority is configured to {}", flowPriority);
    }

    /**
     * Packet processor responsible for forwarding packets along their paths.
     */
    private class ReactivePacketProcessor implements PacketProcessor {

        @Override
        public void process(PacketContext context) {
            // Stop processing if the packet has been handled, since we
            // can't do any more to it.

            if (context.isHandled()) {
                return;
            }

            InboundPacket pkt = context.inPacket();
            Ethernet ethPkt = pkt.parsed();

            if (ethPkt == null) {
                return;
            }

            MacAddress macAddress = ethPkt.getSourceMAC();
            ReactiveForwardMetrics macMetrics = null;
            macMetrics = createCounter(macAddress);
            inPacket(macMetrics);

            // Bail if this is deemed to be a control packet.
            if (isControlPacket(ethPkt)) {
                droppedPacket(macMetrics);
                return;
            }

            // Skip IPv6 multicast packet when IPv6 forward is disabled.
            if (!ipv6Forwarding && isIpv6Multicast(ethPkt)) {
                droppedPacket(macMetrics);
                return;
            }

            HostId id = HostId.hostId(ethPkt.getDestinationMAC());

            // Do not process LLDP MAC address in any way.
            if (id.mac().isLldp()) {
                droppedPacket(macMetrics);
                return;
            }

            // Do not process IPv4 multicast packets, let mfwd handle them
            if (ignoreIPv4Multicast && ethPkt.getEtherType() == Ethernet.TYPE_IPV4) {
                if (id.mac().isMulticast()) {
                    return;
                }
            }

            // Do we know who this is for? If not, flood and bail.
            Host dst = hostService.getHost(id);
            if (dst == null) {
                log.info("--------INITIAL Packet Processor:  dst NULL");
                flood(context, macMetrics);
                return;
            }

            // Are we on an edge switch that our destination is on? If so,
            // simply forward out to the destination and bail.
            if (pkt.receivedFrom().deviceId().equals(dst.location().deviceId())) {
                if (!context.inPacket().receivedFrom().port().equals(dst.location().port())) {
                    installRule(context, dst.location().port(), macMetrics);
                }
                return;
            }
            HostId idsource = HostId.hostId(ethPkt.getSourceMAC());
            // Do we know who this is for? If not, flood and bail.
            Host src = hostService.getHost(idsource);
            if(src!= null) {
                log.info("--------INITIAL Packet Processor:  src: " + src.mac().toString() + " dst :" + dst.mac().toString() + ")");
                createLocalRules(src);
                createLocalRules(dst);
                FLOW_COUNT++;
                deployShortestPath(src, dst);
            }

        }

    }

    /**
     * Host-based optimal pathfinder
     * Deploy FlowRules for Flow (A to B,B to A)
     * This is the a central method, which makes use of the ONOS services and the bestPath() method.
     * It's function is a Host-based optimal pathfinder.
     *
     * @param hostA Host A-Source
     * @param hostB Host B-Destination
     */
    public void deployShortestPath(Host hostA, Host hostB) {

        if (flowRuleExists(hostA, hostB)) {
            log.info("--------INITIAL PATH FINDER: Flow already exists! Not deploying Flow-Rules for Host-combo (" + hostA.mac().toString() + ":" + hostB.mac().toString() + ") again! ");
            return;
        }

        log.info("--------INITIAL PATH FINDER: Evaluating paths for Host-combo (" + hostA.mac().toString() + ":" + hostB.mac().toString() + ")");
        Stream<Path> pstream = topologyService.getKShortestPaths(topologyService.currentTopology(), hostA.location().deviceId(), hostB.location().deviceId());
        Stream<Path> pstream2 = topologyService.getKShortestPaths(topologyService.currentTopology(), hostA.location().deviceId(), hostB.location().deviceId());

        //Convert a Stream to Set
        Set<Path> pathSet = pstream.collect(Collectors.toSet());
        Set<Path> pathSet2 = pstream2.collect(Collectors.toSet());
        log.info("K-Path-size: " + pathSet.size());
        for(Path path : pathSet){

            if(path.cost() > 2.0){
                pathSet2.remove(path);
            }
        }
        Path bestPath = bestPathInSet(pathSet2);

        if (bestPath == null)
            log.info("### I could not find a path between  " + hostA.id() + " and " + hostB.id() + " ###");
        else {
//            // Some Extra Logs for the experiment:
//            String flowName = "";
//            // IF Flow From Host x To Server:
//            if (hostA.ipAddresses().toString().contains("10.0.0.1") || hostB.ipAddresses().toString().contains("10.0.0.1")) flowName = "Flow 1";
//            if (hostA.ipAddresses().toString().contains("10.0.0.2") || hostB.ipAddresses().toString().contains("10.0.0.2")) flowName = "Flow 2";
//            if (hostA.ipAddresses().toString().contains("10.0.0.3") || hostB.ipAddresses().toString().contains("10.0.0.3")) flowName = "Flow 3";
//            if (hostA.ipAddresses().toString().contains("10.0.0.4") || hostB.ipAddresses().toString().contains("10.0.0.4")) flowName = "Flow 4";
//            if (!Objects.equals(flowName, ""))
//            {
//                String nextHopSwitch = bestPath.links().get(0).dst().deviceId().toString();
//                log.info("thesis-log: Deploying " + flowName + ", Nexthop: " + nextHopSwitch + "");
//            }
            setFLows(bestPath.links(), hostA.mac(), hostB.mac());
        }
    }
    /**
     * FlowRuleExists
     * Searchs for existing flow-rules, returns
     *
     * @param hostA
     * @param hostB
     * @return true - if flow already exists, false if not
     */
    public boolean flowRuleExists(Host hostA, Host hostB) {
        // This block searches for already exiting flow rules for this host-pair
        for (FlowRule flowRule : flowRuleService.getFlowEntriesById(appId)) {
            Criterion criterion = flowRule.selector().getCriterion(ETH_SRC);
            MacAddress src = null;
            MacAddress dst = null;

            if (criterion != null)
                src = ((EthCriterion) criterion).mac();
            criterion = flowRule.selector().getCriterion(ETH_DST);
            if (criterion != null)
                dst = ((EthCriterion) criterion).mac();


            if (dst != null && src != null) {
                if (hostA.mac().toString().matches(src.toString()) && hostB.mac().toString().matches(dst.toString())) {
                    return true;
                }
                if (hostA.mac() == dst && hostB.mac() == src) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Host-based optimal pathfinder (scans + connects all Hosts in the system)
     * This is the a central method, which makes use of the ONOS services and the bestPath() method.
     * It's function is a Host-based optimal pathfinder.
     */
    public void laberio() {

        if (runLoadBalance.runNow(PATH_REFRESH_TIME)) {
            log.info("--------------------------------LABERIO STARTS ###");
            /* Querying known Host-Systems from the controller **/
            int linkcount = linkService.getLinkCount();
            log.info("Number of Links : " + linkcount);
            Iterable<Link> linkIterable = linkService.getActiveLinks();
            List<Link> linkList = new ArrayList<>();
            linkIterable.forEach(linkList::add);
            for (Link link : linkList) {
                log.info(link.toString());
            }
            log.info("Number of other links : " + linkList.size());

//            int i = 0;
//            Double[] linkUtilization = new Double[linkList.size()];

            for (Link link : linkList) {

                double load = loadOnLink(link);
                double capacity = LINK_CAPACITY;
                double usagePercent;
                usagePercent = (load * 100 / capacity);
//                linkUtilization[i] = (load / 1000000.0);
                log.info("Usage Percent: " + usagePercent + "%");
//                totalLoad += load;
//                i++;
            }
//            int numberOfFlows = numberOfFlows(linkList) / 4;
//            log.info("**************NUMBER OF FLOWS  : " + numberOfFlows);

//            for (Link link : edgeLinks) {
//                double load = loadOnLink(link);
//                totalLoad += load;
//            }
//            Arrays.sort(linkUtilization, Collections.reverseOrder());

            Set<Link> leaf1links = linkService.getDeviceIngressLinks(DeviceId.deviceId("of:0000000000000003"));
            Double[] linkUtilization2 = new Double[linkList.size()];
            double totalLoad = 0;
            int numberOfFlows = numberOfFlows(leaf1links);
            int k = 0;
            Integer[] flowcount = new Integer[linkList.size()];
            List<Link> leafLinkList = new ArrayList<>();
            for(Link link :leaf1links){
                leafLinkList.add(link);
                double load = loadOnLink(link);
                flowcount[k]= numberOfFlows(link);
                linkUtilization2[k] = (load / 1000000.0);
                totalLoad += load;
                k++;
            }
//            totalLoad = totalLoad / (2*numberOfFlows);
            totalLoad = totalLoad / 1000000.0;

            log.info("Number of Flows : " + numberOfFlows);
            log.info("Total Load : " + totalLoad  + "Mbps");

/*            int size = 0;
            if(numberOfFlows < 1){
                size = 0;
            }else if(numberOfFlows <3){
                size = 2*numberOfFlows;
            }
            else if(numberOfFlows <7){
                size = numberOfFlows +2;
            }
            else{
                size = 8;
            }*/
            int size = 0;
            if(numberOfFlows == 1 || numberOfFlows == 2){
                size = numberOfFlows;
            }
            else if(numberOfFlows >2){
                size = 2;
            }
            double mean = totalLoad / size;
            double sum = 0;
            for (int a = 0; a < size; a++) {
                log.info("Total Load : " + totalLoad  + "link utilization : " + linkUtilization2[a]);
                double tmp = mean - linkUtilization2[a];
                tmp = tmp * tmp;
                sum = sum + tmp;
            }

            log.info("Sum : " + sum + " size = " + size);
            double linkUtilizationVariance = sum / size;
            log.info("Link Utilization Variance : " + linkUtilizationVariance);
            Link objLink = null;
            double threshold = 17.0;
            if(numberOfFlows == 2){
                if(flowcount[0] > flowcount[1]){
                    if(flowcount[0]>flowcount[1]){
                        objLink = leafLinkList.get(0);
                    }else{
                        objLink = leafLinkList.get(1);
                    }
                }
            }
            else if(numberOfFlows > 2) {
                if (linkUtilizationVariance > threshold) {
                    if (linkUtilization2[0] > linkUtilization2[1]) {
                        objLink = leafLinkList.get(0);
                    } else {
                        objLink = leafLinkList.get(1);
                    }
                }
            }
            if(objLink!=null){
                log.info("Object Link SELECTED: " + objLink.toString());

                fixLoadImbalance(objLink);
            }

        }
    }
    private void fixLoadImbalance(Link objLink) {
        Iterable<FlowEntry> flowRule = flowRuleService.getFlowEntries(objLink.src().deviceId());
        FlowEntry objFlow = flowRule.iterator().next();
        log.info("Object FLOW SELECTED: " + objFlow.toString());

        Set<SrcDstPair> pairs = findSrcDstPairs(objFlow);
        log.info("Fixing Load IMBALANCE");
        for (SrcDstPair sd : pairs) {
            // get the edge deviceID for the src host
            Host srcHost = hostService.getHost(HostId.hostId(sd.src));
            Host dstHost = hostService.getHost(HostId.hostId(sd.dst));
            if (srcHost != null && dstHost != null) {
                DeviceId srcId = srcHost.location().deviceId();
                DeviceId dstId = dstHost.location().deviceId();
                log.info("SRC ID is {}, DST ID is {}", srcId, dstId);
                SrcDstPair sdreverse = new SrcDstPair(sd.dst, sd.src);
                cleanFlowRules(sd);
                cleanFlowRules(sdreverse);

                deployShortestPath(srcHost,dstHost, objLink);
            }
        }
    }
    /**
     * Host-based optimal pathfinder
     * Deploy FlowRules for Flow (A to B,B to A)
     * This is the a central method, which makes use of the ONOS services and the bestPath() method.
     * It's function is a Host-based optimal pathfinder.
     *
     * @param hostA Host A-Source
     * @param hostB Host B-Destination
     */
    public void deployShortestPath(Host hostA, Host hostB, Link objLink) {

        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if (flowRuleExists(hostA, hostB)) {
            log.info("--------PATH FINDER FOR LOAD IMBALANCE: Flow already exists! Not deploying Flow-Rules for Host-combo (" + hostA.mac().toString() + ":" + hostB.mac().toString() + ") again! ");
            return;
        }

        log.info("--------PATH FINDER FOR LOAD IMBALANCE:: Evaluating paths for Host-combo (" + hostA.mac().toString() + ":" + hostB.mac().toString() + ")");
        Stream<Path> pstream = topologyService.getKShortestPaths(topologyService.currentTopology(), hostA.location().deviceId(), hostB.location().deviceId());
        Stream<Path> pstream2 = topologyService.getKShortestPaths(topologyService.currentTopology(), hostA.location().deviceId(), hostB.location().deviceId());

        //Convert a Stream to Set
        Set<Path> pathSet = pstream.collect(Collectors.toSet());
        Set<Path> pathSet2 = pstream2.collect(Collectors.toSet());
        log.info("K-Path-size: " + pathSet.size());
        for(Path path : pathSet){

            if(path.cost() > 2.0){
                pathSet2.remove(path);
            }
            else if(path.links().contains(objLink)){
                pathSet2.remove(path);
            }
        }
        Path bestPath = bestPathInSet(pathSet2);

        if (bestPath == null)
            log.info("### I could not find a path between  " + hostA.id() + " and " + hostB.id() + " ###");
        else {
//            // Some Extra Logs for the experiment:
//            String flowName = "";
//            // IF Flow From Host x To Server:
//            if (hostA.ipAddresses().toString().contains("10.0.0.1") || hostB.ipAddresses().toString().contains("10.0.0.1")) flowName = "Flow 1";
//            if (hostA.ipAddresses().toString().contains("10.0.0.2") || hostB.ipAddresses().toString().contains("10.0.0.2")) flowName = "Flow 2";
//            if (hostA.ipAddresses().toString().contains("10.0.0.3") || hostB.ipAddresses().toString().contains("10.0.0.3")) flowName = "Flow 3";
//            if (hostA.ipAddresses().toString().contains("10.0.0.4") || hostB.ipAddresses().toString().contains("10.0.0.4")) flowName = "Flow 4";
//            if (!Objects.equals(flowName, ""))
//            {
//                String nextHopSwitch = bestPath.links().get(0).dst().deviceId().toString();
//                log.info("thesis-log: Deploying " + flowName + ", Nexthop: " + nextHopSwitch + "");
//            }
            setFLows(bestPath.links(), hostA.mac(), hostB.mac());
        }
    }

    // Removes flow rules off specified device with specific SrcDstPair
    private void cleanFlowRules(SrcDstPair pair) {
        log.info("Removing flows w/ SRC={}, DST={}", pair.src, pair.dst);
        for(Device device : deviceService.getDevices())
            for (FlowEntry r : flowRuleService.getFlowEntries(device.id())) {
                boolean matchesSrc = false, matchesDst = false;
                for (Instruction i : r.treatment().allInstructions()) {
                    if (i.type() == Instruction.Type.OUTPUT) {
                        // if the flow has matching src and dst
                        for (Criterion cr : r.selector().criteria()) {
                            if (cr.type() == Criterion.Type.ETH_DST) {
                                if (((EthCriterion) cr).mac().equals(pair.dst)) {
                                    matchesDst = true;
                                }
                            } else if (cr.type() == Criterion.Type.ETH_SRC) {
                                if (((EthCriterion) cr).mac().equals(pair.src)) {
                                    matchesSrc = true;
                                }
                            }
                        }
                    }
                }
                if (matchesDst && matchesSrc) {
                    log.info("Removed flow rule from device: {}", device.id());
                    flowRuleService.removeFlowRules((FlowRule) r);
                }
            }

    }
    private Set<SrcDstPair> findSrcDstPairs(FlowEntry r) {
        ImmutableSet.Builder<SrcDstPair> builder = ImmutableSet.builder();

        MacAddress src = null, dst = null;
        for (Criterion cr : r.selector().criteria()) {
            if (cr.type() == Criterion.Type.ETH_DST) {
                dst = ((EthCriterion) cr).mac();
            } else if (cr.type() == Criterion.Type.ETH_SRC) {
                src = ((EthCriterion) cr).mac();
            }

            builder.add(new SrcDstPair(src, dst));
        }
        return builder.build();
    }

    /**
     * setFLows deploys all the flow-rules at the switches along a linklist
     * Deploy FlowRules for Flow (A to B, B to A)
     *
     * @param linkList A list of those link-objects to deploy rules for
     * @param srcMac   The Flow Source Device
     * @param dstMac   The Flow Destination Device
     */
    public void setFLows(List<Link> linkList, MacAddress srcMac, MacAddress dstMac) {
        for (Link link : linkList) {
            // Flow-Direction: Forward (A->B)
            DeviceId srcSwitch = link.src().deviceId();
            PortNumber outPort = link.src().port();
            setFlow(srcSwitch, srcMac, dstMac, outPort);

            // Flow-Direction: Backward (B->A)
            DeviceId dstSwitch = link.dst().deviceId();
            outPort = link.dst().port();
            setFlow(dstSwitch, dstMac, srcMac, outPort);
        }
    }
    /**
     * overloads original setFlow with default timeout parameter
     *
     * @param switchDevice ID of Switch to deploy to
     * @param srcMac       The Flow Source Device
     * @param dstMac       The Flow Destination Device
     * @param outPort      The Flow Forwarding Out:Port
     */
    public void setFlow(DeviceId switchDevice, MacAddress srcMac, MacAddress dstMac, PortNumber outPort) {
        setFlow(switchDevice, srcMac, dstMac, outPort, DEFAULT_RULE_TIMEOUT);
    }

    /**
     * overloads original setFlow with default priority parameter
     *
     * @param switchDevice ID of Switch to deploy to
     * @param srcMac       The Flow Source Device
     * @param dstMac       The Flow Destination Device
     * @param outPort      The Flow Forwarding Out:Port
     * @param timeout      The Flow-(Soft)-Timeout
     */
    public void setFlow(DeviceId switchDevice, MacAddress srcMac, MacAddress dstMac, PortNumber outPort, int timeout) {
        setFlow(switchDevice, srcMac, dstMac, outPort, timeout, DEFAULT_RULE_PRIO);
    }
    /**
     * Helper Method, pushs FlowRules to Switching Devices
     *
     * @param switchDevice ID of Switch to deploy to
     * @param srcMac       The Flow Source Device
     * @param dstMac       The Flow Destination Device
     * @param outPort      The Flow Forwarding Out:Port
     * @param timeout      The Flow-(Soft)-Timeout
     * @param flowPriority The Flow Priority
     */
    public void setFlow(DeviceId switchDevice, MacAddress srcMac, MacAddress dstMac, PortNumber outPort, int timeout, int flowPriority) {

        /*  Define which packets(flows) to handle **/
        TrafficSelector.Builder selectorBuilder = DefaultTrafficSelector.builder();
        /*  if no src-Mac is defined forward disregarding source address (used in local switch rules) */
        if (srcMac != null) {
            selectorBuilder.matchEthSrc(srcMac);
        }
        selectorBuilder.matchEthDst(dstMac);

        /*  Define what to do with the packet / flow **/
        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                .setOutput(outPort)
                .build();

        /*  add Flow-Priority, Timeout, Flags and some other meta information **/
        ForwardingObjective forwardingObjective = DefaultForwardingObjective.builder()
                .withSelector(selectorBuilder.build())
                .withTreatment(treatment)
                .withPriority(flowPriority)
                .withFlag(ForwardingObjective.Flag.VERSATILE)
                .fromApp(appId)
                .makeTemporary(timeout)
                .add();
        if (srcMac == null)
            log.info("### Add Flow at Switch " + switchDevice.toString() + " dst: " + dstMac.toString() + " outPort: " + outPort.toString() + " ###");
        if (srcMac != null)
            log.info("### Add Flow at Switch " + switchDevice.toString() + " src: " + srcMac.toString() + " dst: " + dstMac.toString() + " outPort: " + outPort.toString() + " ###");
        flowObjectiveService.forward(switchDevice, forwardingObjective);
    }

    public double loadOnLink(Link link) {

        DeviceId dst_id = link.dst().deviceId();
        DeviceId src_id = link.src().deviceId();
        Port dst_port = deviceService.getPort(link.dst());
        Port src_port = deviceService.getPort(link.src());

        PortStatistics srcPortStats = deviceService.getDeltaStatisticsForPort(src_id, src_port.number());
        PortStatistics dstPortStats = deviceService.getDeltaStatisticsForPort(dst_id, dst_port.number());

        float rcvRate = ((dstPortStats.bytesReceived() * 8));
        long rcvPktDrops = dstPortStats.packetsRxDropped();
        float sntRate = ((srcPortStats.bytesSent() * 8));
        long sntPktDrops = srcPortStats.packetsTxDropped();
        //log.info("LOAD ON LINK With link :" + link.toString()+" is: "+ rcvRate);
        return rcvRate;
    }

    /**
     * maxUsagePercentOnPath
     * Returns the highest usage percent on the path
     *
     * @param path
     * @return the highest usage percent on the path
     */
    public double maxUsagePercentOnPath(Path path) {
        double maxUsage = 0;
        for (Link link : path.links()) {
            double load = loadOnLink(link);
            double capacity = capacityOfLink(link);
            double usagePercent;
            if (capacity > 0) usagePercent = (load * 100 / capacity);
            else usagePercent = 100;

            maxUsage = usagePercent;
        }
        return maxUsage;
    }

    /**
     * minFreeOnPath
     * Return the smallest free capacity on the path
     *
     * @param path
     * @return the smallest free capacity on the path
     */
    public double minFreeOnPath(Path path) {
        double minFree = -7411.0;
        for (Link link : path.links()) {
            double load = loadOnLink(link);
            double capacity = capacityOfLink(link);
            // Brutto-Capacity to expected Netto-Capacity.
            double nettoCapacity = bruttoToNetto(capacity);
            double free = capacity - load;//double free = nettoCapacity - load;
            log.info("brutto-cap: " + capacity);
            log.info("netto-cap: " + nettoCapacity);
            log.info("load: " + load);
            // If first run: Set minfree = free
            if (minFree == -7411.0) minFree = free;
            // If cap-load is smaller than 0> set to 0.
            if (free < 0) {
                log.info("There is more Load(" + load + ") than Capacity(" + nettoCapacity + ")");
                free = 0;
            }
            // if this free is smaller than the previously smallest, set minfree = free.
            if (free < minFree) {
                minFree = free;
            }
        }
        return minFree;
    }

    /**
     * Count the Flow's, currently deployed on this path. Only Flows matching Devices and outports are counted.
     *
     * @param path The path to evaluate
     * @return The number of Flows
     */
    public int numberOfFlowsOnPath(Path path) {
        int flowRuleCounter = 0;
        for (Link link : path.links()) {

            /* Query FlowRules for every "Source"-Device at the links of the Path **/
            /* Querying only Destination Devices, as the only important nodes are the nexthop nodes **/
            for (FlowRule flowRule : flowRuleService.getFlowEntries(link.src().deviceId())) {

                /* If this Rule was set by this application  **/
                if (flowRule.appId() == appId.id()) {

                    /* If this Rule's Instruction is to forward on the same port as the 'path' **/
                    boolean portMatch = false;
                    //noinspection EmptyCatchBlock
                    try {
                        if (flowRule.treatment().allInstructions().get(0).toString().equals("OUTPUT:" + link.src().port().toLong())) {
                            portMatch = true;
                        }

                    } catch (NullPointerException npe) {
                        log.info("NullPointerException: " + npe.getLocalizedMessage());
                    }
                    if (portMatch) {
                        log.info("### Count: Flow-Rule at Device " + link.src().toString() + ", matches Portno: " + link.src().port().toLong());
                        flowRuleCounter++;
                    }
                }
            }
        }
        return flowRuleCounter;
    }

    /**
     * Count the Flow's, currently deployed on this path. Only Flows matching Devices and outports are counted.
     *
     * @param links The links to evaluate
     * @return The number of Flows
     */
    public int numberOfFlows(List<Link> links) {
        int flowRuleCounter = 0;
        for (Link link : links) {

            /* Query FlowRules for every "Source"-Device at the links of the Path **/
            /* Querying only Destination Devices, as the only important nodes are the nexthop nodes **/
            for (FlowRule flowRule : flowRuleService.getFlowEntries(link.src().deviceId())) {

                /* If this Rule was set by this application  **/
                if (flowRule.appId() == appId.id()) {

                    /* If this Rule's Instruction is to forward on the same port as the 'path' **/
                    boolean portMatch = false;
                    //noinspection EmptyCatchBlock
                    try {
                        if (flowRule.treatment().allInstructions().get(0).toString().equals("OUTPUT:" + link.src().port().toLong())) {
                            portMatch = true;
                        }

                    } catch (NullPointerException npe) {
                        log.info("NullPointerException: " + npe.getLocalizedMessage());
                    }
                    if (portMatch) {
                        log.info("### Count: Flow-Rule at Device " + link.src().toString() + ", matches Portno: " + link.src().port().toLong());
                        flowRuleCounter++;
                    }
                }
            }
        }
        return flowRuleCounter;
    }
    /**
     * Count the Flow's, currently deployed on this path. Only Flows matching Devices and outports are counted.
     *
     * @param link The link to evaluate
     * @return The number of Flows
     */
    public int numberOfFlows(Link link) {
        int flowRuleCounter = 0;
        /* Query FlowRules for every "Source"-Device at the links of the Path **/
        /* Querying only Destination Devices, as the only important nodes are the nexthop nodes **/
        for (FlowRule flowRule : flowRuleService.getFlowEntries(link.src().deviceId())) {

            /* If this Rule was set by this application  **/
            if (flowRule.appId() == appId.id()) {

                /* If this Rule's Instruction is to forward on the same port as the 'path' **/
                boolean portMatch = false;
                //noinspection EmptyCatchBlock
                try {
                    if (flowRule.treatment().allInstructions().get(0).toString().equals("OUTPUT:" + link.src().port().toLong())) {
                        portMatch = true;
                    }

                } catch (NullPointerException npe) {
                    log.info("NullPointerException: " + npe.getLocalizedMessage());
                }
                if (portMatch) {
                    log.info("### Count: Flow-Rule at Device " + link.src().toString() + ", matches Portno: " + link.src().port().toLong());
                    flowRuleCounter++;
                }
            }

        }
        return flowRuleCounter;
    }
    /**
     * Count the Flow's, currently deployed on this path. Only Flows matching Devices and outports are counted.
     *
     * @param links The links to evaluate
     * @return The number of Flows
     */
    public int numberOfFlows(Set<Link> links) {
        int flowRuleCounter = 0;
        for (Link link : links) {

            /* Query FlowRules for every "Source"-Device at the links of the Path **/
            /* Querying only Destination Devices, as the only important nodes are the nexthop nodes **/
            for (FlowRule flowRule : flowRuleService.getFlowEntries(link.src().deviceId())) {

                /* If this Rule was set by this application  **/
                if (flowRule.appId() == appId.id()) {

                    /* If this Rule's Instruction is to forward on the same port as the 'path' **/
                    boolean portMatch = false;
                    //noinspection EmptyCatchBlock
                    try {
                        if (flowRule.treatment().allInstructions().get(0).toString().equals("OUTPUT:" + link.src().port().toLong())) {
                            portMatch = true;
                        }

                    } catch (NullPointerException npe) {
                        log.info("NullPointerException: " + npe.getLocalizedMessage());
                    }
                    if (portMatch) {
                        log.info("### Count: Flow-Rule at Device " + link.src().toString() + ", matches Portno: " + link.src().port().toLong());
                        flowRuleCounter++;
                    }
                }
            }
        }
        return flowRuleCounter;
    }
    /**
     * Calculate and return the least loaded path from a given Set of paths
     *
     * @param paths Set of paths, to find best one in.
     * @return The least loaded path
     */
    public Path bestPathInSet(Set<Path> paths) {
        log.info("Starting now");
        // If the Set is empty, return null
        int count_paths = paths.size();

        if (count_paths == 0) return null;
        Path bestPath = paths.iterator().next();

        if (count_paths == 1) return bestPath;
        double[] maxLinkUsage = new double[count_paths];
        double[] minFreeonPath = new double[count_paths];
        int[] hopcount = new int[count_paths];
        int[] numberOfFlows = new int[count_paths];
        Path[] pathList = new Path[count_paths];


        int i = 0;

        for (Path path : paths) {
            pathList[i] = path;
            maxLinkUsage[i] = maxUsagePercentOnPath(path); //=> The  usage percent of the link with the highest usage on the path.
            minFreeonPath[i] = minFreeOnPath(path); // => The free capacity of the link with the least free capacity on the path.
            hopcount[i] = (int) path.cost(); // => the path-hopcount metric
            numberOfFlows[i] = numberOfFlowsOnPath(path); // => The number of flows set on this path.
            log.info("Path-Log: No. " + i + ", nexthop: " + path.links().iterator().next().dst().deviceId() + ", minFree: " + minFreeonPath[i] + ", maxusage: " + maxLinkUsage[i] + ", NumberOfFLows: " + numberOfFlows[i] + ", hopcount: " + hopcount[i]);
            i++;
        }


        //Different evaluation scenarios:
        //1: Only Hopcount
        //2: Hopcount + Flowcount
        //3: Hopcount + Flowcount + UsagePercentage

        if (test_scenario == 4) {
            log.info("Path decision based on scenario 4: Using (Capacity-Load), Flowcount and Hopcount !!!");
            /** PATH DECISION: 1. compare usage-percent ***/
            /* Use a helper method to get the index of the less-loaded path in the Load-Array */
            if (biggestMinFreeOnPath(minFreeonPath) == -1) {
                return bestPath;
            } else // minFree differs
            {
                int index = biggestMinFreeOnPath(minFreeonPath);
                bestPath = pathList[index];
                log.info("thesis-log: Choosing path with the highest unused capacity! HOPCOUNT ="+ hopcount[index]+ "(" + minFreeonPath[index] + ") Bestpath: " + bestPath.toString());
                return bestPath;
            }
        }

        return bestPath;
    }


    /**
     * Find the smallest array-field and return the index. If all Fields are the same, return -1.
     *
     * @param array The Path-Array
     * @return the array-index of the smallest field
     */
    public static int smallestNumberFromArray(int[] array) {
        OptionalInt max = IntStream.of(array).max();
        OptionalInt min = IntStream.of(array).min();
        int min_id = IntStream.of(array).boxed().collect(toList()).indexOf(min.getAsInt());
        // IF the smallest int is equal with the biggest one, return -1, else return index of the smallest one.
        if (max.getAsInt() == min.getAsInt()) return -1;
        else return min_id;
    }

    /**
     * Calculate the approximate netto-capacity on a link, given the brutto data-rate, respectively its modulation mode.
     *
     * @param capacity The brutto capacity
     * @return expected netto-capacity
     */
    public double bruttoToNetto(double capacity) {
        double netto;
        double x = 1;
        /* The following x -factors are based on measurements in each topology and with each data-rate **/
        /* Its goal is to get a capacity value, that is as close as possible at the according load-value reported by the port-stats **/
        switch ((int) capacity) {
            case 6000000:
                x = 1.9;
                break;
            case 12000000:
                x = 1.625;
                break;
            case 24000000:
                x = 1.333;
                break;
            case 36000000:
                x = 1.16;
                break;
            case 54000000:
                x = 0.943;
                break;
        }
        netto = capacity / 100 * x;
        return netto;
    }

    /**
     * Returns the Index of the entry with the smallest Usage-Percent. If all entrys are similar (delta 5 %) return -1.
     *
     * @param array (UsagePercentArray)
     * @return the index of the smallest usagepercent field
     */
    public int smallestUsagePercentFromArray(double[] array) {
        OptionalDouble max = DoubleStream.of(array).max();
        OptionalDouble min = DoubleStream.of(array).min();
        int min_id = DoubleStream.of(array).boxed().collect(toList()).indexOf(min.getAsDouble());
        // If the smallest and the biggest values are more close as 5,  return -1, else return index of the smallest one.
        log.info("## UsagePercent comparison: " + max.getAsDouble() + " vs. " + min.getAsDouble());
        if (Math.abs(max.getAsDouble() - min.getAsDouble()) < 0.05) return -1;
        else return min_id;
    }

    /**
     * biggestFreeeOnPath
     * returns the index of the field with the  highest free capacity value
     *
     * @param array - the array to search in
     * @return the index of the field with the  highest free capacity value
     */
    public int biggestMinFreeOnPath(double[] array) {
        // first sort, than find min and max. => Choose max.
        OptionalDouble max = DoubleStream.of(array).max();
        OptionalDouble min = DoubleStream.of(array).min();

        int max_id = DoubleStream.of(array).boxed().collect(toList()).indexOf(max.getAsDouble());
        // If min = max, than return -1 (All paths are equal)
        log.info("Max Free = " + max.getAsDouble() + ", Min_Free= " + min.getAsDouble());
        // 2000 is approx 3%. Which is used as saturation of a link is not always the same.
        if (Math.abs(max.getAsDouble() - min.getAsDouble()) < 2000) return -1;
        //otherwise return path id with max free
        return max_id;
    }

    /**
     * As ONOS does not know the Mac-Address of a Switch-Port, This method re
     * The mac-addressing scheme is the same on the mininet/ns3 site
     *
     * @param switchPort (ConnectionPoint)
     * @return The belonging MAC-Address, empty String of Mac unknown.
     */
    public String macaddressOfSwitchPort(ConnectPoint switchPort) {
        long port = switchPort.port().toLong();
        String switchID = switchPort.deviceId().toString();
        switch (switchID) {
            // MR1
            case ("of:0000000000000101"): {
                if (port == 1) return "00:00:00:00:01:01";
                if (port == 2) return "00:00:00:00:01:02";
                if (port == 3) return "00:00:00:00:01:03";
                if (port == 4) return "00:00:00:00:01:04";
                break;
            }
            // MR2
            case ("of:0000000000000201"): {
                if (port == 1) return "00:00:00:00:02:01";
                if (port == 2) return "00:00:00:00:02:02";
                if (port == 3) return "00:00:00:00:02:03";
                if (port == 4) return "00:00:00:00:02:04";
                break;
            }
            // MR3
            case ("of:0000000000000301"): {
                if (port == 1) return "00:00:00:00:03:01";
                if (port == 2) return "00:00:00:00:03:02";
                if (port == 3) return "00:00:00:00:03:03";
                if (port == 4) return "00:00:00:00:03:04";
                break;
            }
            // MR4
            case ("of:0000000000000401"): {
                if (port == 1) return "00:00:00:00:04:01";
                if (port == 2) return "00:00:00:00:04:02";
                if (port == 3) return "00:00:00:00:04:03";
                if (port == 4) return "00:00:00:00:04:04";
                break;
            }
            // MGW1
            case ("of:0000000000000501"): {
                if (port == 1) return "00:00:00:00:05:01";
                if (port == 2) return "00:00:00:00:05:02";
                if (port == 3) return "00:00:00:00:05:03";
                if (port == 4) return "00:00:00:00:05:04";
                break;
            }
        }
        log.info("!!! There is no Mac-Address known for the sw/port combo: " + switchID + " " + port);
        return "";
    }

    /**
     * capacityOfLink
     * Reads the current link-capacity from a file that is constantly updated by ns-3 (capacity-patch)
     *
     * @param link - the concerned link
     * @return the current link capacity
     */
    public int capacityOfLink(Link link) {
        // Default-Rate, if there is no reported capacity. A  mid-quality link is assumed.
        int rate = 20000000;
/*        ConnectPoint switchPort = link.src();
        String macAddress = macaddressOfSwitchPort(switchPort);

        *//* Read corresponding File from local filesystem (Previously written by ns-3 simulation) **//*
        java.nio.file.Path path = FileSystems.getDefault().getPath("/tmp/ns-3/" + macAddress + ".log");
        if (Objects.equals(macAddress, "")){
            log.info("No MAC specified, No DataRate to query");
        }
        else {
            log.info("### Trying to open : " + "/tmp/ns-3/" + macAddress + ".log ###");
            try (InputStream is = Files.newInputStream(path, StandardOpenOption.READ)) {
                InputStreamReader reader = new InputStreamReader(is);
                BufferedReader lineReader = new BufferedReader(reader);
                String line;

                while ((line = lineReader.readLine()) != null) {

                    if (line != null) {
                        rate = Integer.parseInt(line);
                    }
                }

            } catch (IOException io) {
                log.info(io.getMessage());
            }
        }*/
        if (rate == 33000000)
            log.info("It seems like there was no datarate information for this mac.");
        else {
            log.info("Got capacity rate: " + rate);
        }
        return rate;
    }

    // Indicates whether this is a control packet, e.g. LLDP, BDDP
    private boolean isControlPacket(Ethernet eth) {
        short type = eth.getEtherType();
        return type == Ethernet.TYPE_LLDP || type == Ethernet.TYPE_BSN;
    }

    // Indicated whether this is an IPv6 multicast packet.
    private boolean isIpv6Multicast(Ethernet eth) {
        return eth.getEtherType() == Ethernet.TYPE_IPV6 && eth.isMulticast();
    }

    // Selects a path from the given set that does not lead back to the
    // specified port if possible.
    private Path pickForwardPathIfPossible(Set<Path> paths, PortNumber notToPort) {
        for (Path path : paths) {
            if (!path.src().port().equals(notToPort)) {
                return path;
            }
        }
        return null;
    }

    // Floods the specified packet if permissible.
    private void flood(PacketContext context, ReactiveForwardMetrics macMetrics) {
        if (topologyService.isBroadcastPoint(topologyService.currentTopology(),
                                             context.inPacket().receivedFrom())) {
            packetOut(context, PortNumber.FLOOD, macMetrics);
        } else {
            context.block();
        }
    }

    // Sends a packet out the specified port.
    private void packetOut(PacketContext context, PortNumber portNumber, ReactiveForwardMetrics macMetrics) {
        replyPacket(macMetrics);
        context.treatmentBuilder().setOutput(portNumber);
        context.send();
    }

    // Install a rule forwarding the packet to the specified port.
    private void installRule(PacketContext context, PortNumber portNumber, ReactiveForwardMetrics macMetrics) {
        //
        // We don't support (yet) buffer IDs in the Flow Service so
        // packet out first.
        //
        Ethernet inPkt = context.inPacket().parsed();
        TrafficSelector.Builder selectorBuilder = DefaultTrafficSelector.builder();

        // If PacketOutOnly or ARP packet than forward directly to output port
        if (packetOutOnly || inPkt.getEtherType() == Ethernet.TYPE_ARP) {
            packetOut(context, portNumber, macMetrics);
            return;
        }

        //
        // If matchDstMacOnly
        //    Create flows matching dstMac only
        // Else
        //    Create flows with default matching and include configured fields
        //
        if (matchDstMacOnly) {
            selectorBuilder.matchEthDst(inPkt.getDestinationMAC());
        } else {
            selectorBuilder.matchInPort(context.inPacket().receivedFrom().port())
                    .matchEthSrc(inPkt.getSourceMAC())
                    .matchEthDst(inPkt.getDestinationMAC());

            // If configured Match Vlan ID
            if (matchVlanId && inPkt.getVlanID() != Ethernet.VLAN_UNTAGGED) {
                selectorBuilder.matchVlanId(VlanId.vlanId(inPkt.getVlanID()));
            }

            //
            // If configured and EtherType is IPv4 - Match IPv4 and
            // TCP/UDP/ICMP fields
            //
            if (matchIpv4Address && inPkt.getEtherType() == Ethernet.TYPE_IPV4) {
                IPv4 ipv4Packet = (IPv4) inPkt.getPayload();
                byte ipv4Protocol = ipv4Packet.getProtocol();
                Ip4Prefix matchIp4SrcPrefix =
                        Ip4Prefix.valueOf(ipv4Packet.getSourceAddress(),
                                          Ip4Prefix.MAX_MASK_LENGTH);
                Ip4Prefix matchIp4DstPrefix =
                        Ip4Prefix.valueOf(ipv4Packet.getDestinationAddress(),
                                          Ip4Prefix.MAX_MASK_LENGTH);
                selectorBuilder.matchEthType(Ethernet.TYPE_IPV4)
                        .matchIPSrc(matchIp4SrcPrefix)
                        .matchIPDst(matchIp4DstPrefix);

                if (matchIpv4Dscp) {
                    byte dscp = ipv4Packet.getDscp();
                    byte ecn = ipv4Packet.getEcn();
                    selectorBuilder.matchIPDscp(dscp).matchIPEcn(ecn);
                }

                if (matchTcpUdpPorts && ipv4Protocol == IPv4.PROTOCOL_TCP) {
                    TCP tcpPacket = (TCP) ipv4Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv4Protocol)
                            .matchTcpSrc(TpPort.tpPort(tcpPacket.getSourcePort()))
                            .matchTcpDst(TpPort.tpPort(tcpPacket.getDestinationPort()));
                }
                if (matchTcpUdpPorts && ipv4Protocol == IPv4.PROTOCOL_UDP) {
                    UDP udpPacket = (UDP) ipv4Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv4Protocol)
                            .matchUdpSrc(TpPort.tpPort(udpPacket.getSourcePort()))
                            .matchUdpDst(TpPort.tpPort(udpPacket.getDestinationPort()));
                }
                if (matchIcmpFields && ipv4Protocol == IPv4.PROTOCOL_ICMP) {
                    ICMP icmpPacket = (ICMP) ipv4Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv4Protocol)
                            .matchIcmpType(icmpPacket.getIcmpType())
                            .matchIcmpCode(icmpPacket.getIcmpCode());
                }
            }

            //
            // If configured and EtherType is IPv6 - Match IPv6 and
            // TCP/UDP/ICMP fields
            //
            if (matchIpv6Address && inPkt.getEtherType() == Ethernet.TYPE_IPV6) {
                IPv6 ipv6Packet = (IPv6) inPkt.getPayload();
                byte ipv6NextHeader = ipv6Packet.getNextHeader();
                Ip6Prefix matchIp6SrcPrefix =
                        Ip6Prefix.valueOf(ipv6Packet.getSourceAddress(),
                                          Ip6Prefix.MAX_MASK_LENGTH);
                Ip6Prefix matchIp6DstPrefix =
                        Ip6Prefix.valueOf(ipv6Packet.getDestinationAddress(),
                                          Ip6Prefix.MAX_MASK_LENGTH);
                selectorBuilder.matchEthType(Ethernet.TYPE_IPV6)
                        .matchIPv6Src(matchIp6SrcPrefix)
                        .matchIPv6Dst(matchIp6DstPrefix);

                if (matchIpv6FlowLabel) {
                    selectorBuilder.matchIPv6FlowLabel(ipv6Packet.getFlowLabel());
                }

                if (matchTcpUdpPorts && ipv6NextHeader == IPv6.PROTOCOL_TCP) {
                    TCP tcpPacket = (TCP) ipv6Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv6NextHeader)
                            .matchTcpSrc(TpPort.tpPort(tcpPacket.getSourcePort()))
                            .matchTcpDst(TpPort.tpPort(tcpPacket.getDestinationPort()));
                }
                if (matchTcpUdpPorts && ipv6NextHeader == IPv6.PROTOCOL_UDP) {
                    UDP udpPacket = (UDP) ipv6Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv6NextHeader)
                            .matchUdpSrc(TpPort.tpPort(udpPacket.getSourcePort()))
                            .matchUdpDst(TpPort.tpPort(udpPacket.getDestinationPort()));
                }
                if (matchIcmpFields && ipv6NextHeader == IPv6.PROTOCOL_ICMP6) {
                    ICMP6 icmp6Packet = (ICMP6) ipv6Packet.getPayload();
                    selectorBuilder.matchIPProtocol(ipv6NextHeader)
                            .matchIcmpv6Type(icmp6Packet.getIcmpType())
                            .matchIcmpv6Code(icmp6Packet.getIcmpCode());
                }
            }
        }
        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                .setOutput(portNumber)
                .build();

        ForwardingObjective forwardingObjective = DefaultForwardingObjective.builder()
                .withSelector(selectorBuilder.build())
                .withTreatment(treatment)
                .withPriority(flowPriority)
                .withFlag(ForwardingObjective.Flag.VERSATILE)
                .fromApp(appId)
                .makeTemporary(flowTimeout)
                .add();

        flowObjectiveService.forward(context.inPacket().receivedFrom().deviceId(),
                                     forwardingObjective);
        forwardPacket(macMetrics);
        //
        // If packetOutOfppTable
        //  Send packet back to the OpenFlow pipeline to match installed flow
        // Else
        //  Send packet direction on the appropriate port
        //
        if (packetOutOfppTable) {
            packetOut(context, PortNumber.TABLE, macMetrics);
        } else {
            packetOut(context, portNumber, macMetrics);
        }
    }

    /**
     * Querys a List of Hosts from Onos-Hostservice, Iterates through test List, pushs FlowRules to the Switching devices they are connected to
     */
    public void createLocalRules() {
        /* Querying known Host-Systems from the controller **/
        Iterator<Host> hostIterator = hostService.getHosts().iterator();

        while (hostIterator.hasNext()) {
            Host hostA = hostIterator.next();
            log.info("### Setting local Flows for Host: " + hostA.mac().toString() + " ###");
            setFlow(hostA.location().deviceId(), null, hostA.mac(), hostA.location().port(), LOCAL_RULE_TIMEOUT, LOCAL_RULE_PRIO);
        }
    }
    /**
     * Push "local" FlowRules to the Switching device to provide connectivity
     *
     * @param host Host, for which deploy a local rule for.
     */
    public void createLocalRules(Host host) {
        log.info("### Setting local Flows for Host: " + host.mac().toString() + " ###");
        setFlow(host.location().deviceId(), null, host.mac(), host.location().port(), LOCAL_RULE_TIMEOUT, LOCAL_RULE_PRIO);
    }

    private class InternalTopologyListener implements TopologyListener {
        @Override
        public void event(TopologyEvent event) {
            List<Event> reasons = event.reasons();
            if (reasons != null) {
                reasons.forEach(re -> {
                    if (re instanceof LinkEvent) {
                        LinkEvent le = (LinkEvent) re;
                        if (le.type() == LinkEvent.Type.LINK_REMOVED && blackHoleExecutor != null) {
                            blackHoleExecutor.submit(() -> fixBlackhole(le.subject().src()));
                        }
                    }
                });
            }
        }
    }

    private void fixBlackhole(ConnectPoint egress) {
        Set<FlowEntry> rules = getFlowRulesFrom(egress);
        Set<SrcDstPair> pairs = findSrcDstPairs(rules);

        Map<DeviceId, Set<Path>> srcPaths = new HashMap<>();

        for (SrcDstPair sd : pairs) {
            // get the edge deviceID for the src host
            Host srcHost = hostService.getHost(HostId.hostId(sd.src));
            Host dstHost = hostService.getHost(HostId.hostId(sd.dst));
            if (srcHost != null && dstHost != null) {
                DeviceId srcId = srcHost.location().deviceId();
                DeviceId dstId = dstHost.location().deviceId();
                log.trace("SRC ID is {}, DST ID is {}", srcId, dstId);

                cleanFlowRules(sd, egress.deviceId());

                Set<Path> shortestPaths = srcPaths.get(srcId);
                if (shortestPaths == null) {
                    shortestPaths = topologyService.getPaths(topologyService.currentTopology(),
                                                             egress.deviceId(), srcId);
                    srcPaths.put(srcId, shortestPaths);
                }
                backTrackBadNodes(shortestPaths, dstId, sd);
            }
        }
    }

    // Backtracks from link down event to remove flows that lead to blackhole
    private void backTrackBadNodes(Set<Path> shortestPaths, DeviceId dstId, SrcDstPair sd) {
        for (Path p : shortestPaths) {
            List<Link> pathLinks = p.links();
            for (int i = 0; i < pathLinks.size(); i = i + 1) {
                Link curLink = pathLinks.get(i);
                DeviceId curDevice = curLink.src().deviceId();

                // skipping the first link because this link's src has already been pruned beforehand
                if (i != 0) {
                    cleanFlowRules(sd, curDevice);
                }

                Set<Path> pathsFromCurDevice =
                        topologyService.getPaths(topologyService.currentTopology(),
                                                 curDevice, dstId);
                if (pickForwardPathIfPossible(pathsFromCurDevice, curLink.src().port()) != null) {
                    break;
                } else {
                    if (i + 1 == pathLinks.size()) {
                        cleanFlowRules(sd, curLink.dst().deviceId());
                    }
                }
            }
        }
    }

    // Removes flow rules off specified device with specific SrcDstPair
    private void cleanFlowRules(SrcDstPair pair, DeviceId id) {
        log.trace("Searching for flow rules to remove from: {}", id);
        log.trace("Removing flows w/ SRC={}, DST={}", pair.src, pair.dst);
        for (FlowEntry r : flowRuleService.getFlowEntries(id)) {
            boolean matchesSrc = false, matchesDst = false;
            for (Instruction i : r.treatment().allInstructions()) {
                if (i.type() == Instruction.Type.OUTPUT) {
                    // if the flow has matching src and dst
                    for (Criterion cr : r.selector().criteria()) {
                        if (cr.type() == Criterion.Type.ETH_DST) {
                            if (((EthCriterion) cr).mac().equals(pair.dst)) {
                                matchesDst = true;
                            }
                        } else if (cr.type() == Criterion.Type.ETH_SRC) {
                            if (((EthCriterion) cr).mac().equals(pair.src)) {
                                matchesSrc = true;
                            }
                        }
                    }
                }
            }
            if (matchesDst && matchesSrc) {
                log.trace("Removed flow rule from device: {}", id);
                flowRuleService.removeFlowRules((FlowRule) r);
            }
        }

    }

    // Returns a set of src/dst MAC pairs extracted from the specified set of flow entries
    private Set<SrcDstPair> findSrcDstPairs(Set<FlowEntry> rules) {
        ImmutableSet.Builder<SrcDstPair> builder = ImmutableSet.builder();
        for (FlowEntry r : rules) {
            MacAddress src = null, dst = null;
            for (Criterion cr : r.selector().criteria()) {
                if (cr.type() == Criterion.Type.ETH_DST) {
                    dst = ((EthCriterion) cr).mac();
                } else if (cr.type() == Criterion.Type.ETH_SRC) {
                    src = ((EthCriterion) cr).mac();
                }
            }
            builder.add(new SrcDstPair(src, dst));
        }
        return builder.build();
    }

    private ReactiveForwardMetrics createCounter(MacAddress macAddress) {
        ReactiveForwardMetrics macMetrics = null;
        if (recordMetrics) {
            macMetrics = metrics.compute(macAddress, (key, existingValue) -> {
                if (existingValue == null) {
                    return new ReactiveForwardMetrics(0L, 0L, 0L, 0L, macAddress);
                } else {
                    return existingValue;
                }
            });
        }
        return macMetrics;
    }

    private void  forwardPacket(ReactiveForwardMetrics macmetrics) {
        if (recordMetrics) {
            macmetrics.incrementForwardedPacket();
            metrics.put(macmetrics.getMacAddress(), macmetrics);
        }
    }

    private void inPacket(ReactiveForwardMetrics macmetrics) {
        if (recordMetrics) {
            macmetrics.incrementInPacket();
            metrics.put(macmetrics.getMacAddress(), macmetrics);
        }
    }

    private void replyPacket(ReactiveForwardMetrics macmetrics) {
        if (recordMetrics) {
            macmetrics.incremnetReplyPacket();
            metrics.put(macmetrics.getMacAddress(), macmetrics);
        }
    }

    private void droppedPacket(ReactiveForwardMetrics macmetrics) {
        if (recordMetrics) {
            macmetrics.incrementDroppedPacket();
            metrics.put(macmetrics.getMacAddress(), macmetrics);
        }
    }

    public EventuallyConsistentMap<MacAddress, ReactiveForwardMetrics> getMacAddress() {
        return metrics;
    }

    public void printMetric(MacAddress mac) {
        System.out.println("-----------------------------------------------------------------------------------------");
        System.out.println(" MACADDRESS \t\t\t\t\t\t Metrics");
        if (mac != null) {
            System.out.println(" " + mac + " \t\t\t " + metrics.get(mac));
        } else {
            for (MacAddress key : metrics.keySet()) {
                System.out.println(" " + key + " \t\t\t " + metrics.get(key));
            }
        }
    }

    private Set<FlowEntry> getFlowRulesFrom(ConnectPoint egress) {
        ImmutableSet.Builder<FlowEntry> builder = ImmutableSet.builder();
        flowRuleService.getFlowEntries(egress.deviceId()).forEach(r -> {
            if (r.appId() == appId.id()) {
                r.treatment().allInstructions().forEach(i -> {
                    if (i.type() == Instruction.Type.OUTPUT) {
                        if (((Instructions.OutputInstruction) i).port().equals(egress.port())) {
                            builder.add(r);
                        }
                    }
                });
            }
        });

        return builder.build();
    }

    private class executionSynchronisation {
        Date lastExecutionTime;

        public executionSynchronisation() {
            lastExecutionTime = null;
        }

        public boolean runNow(long refreshTime) {
            Date now = new Date();
            //init with refreshtime, so in case of first run it will return true
            long diff = refreshTime;
            if (lastExecutionTime != null)
                diff = (now.getTime() - lastExecutionTime.getTime()) / 1000;

            if (diff >= refreshTime) {
                /* In case of first execution or last execution was long enough ago: Set Date and go on **/
                lastExecutionTime = new Date();
                return true;
            } else return false;

        }
    }

    // Wrapper class for a source and destination pair of MAC addresses
    private final class SrcDstPair {
        final MacAddress src;
        final MacAddress dst;

        private SrcDstPair(MacAddress src, MacAddress dst) {
            this.src = src;
            this.dst = dst;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SrcDstPair that = (SrcDstPair) o;
            return Objects.equals(src, that.src) &&
                    Objects.equals(dst, that.dst);
        }

        @Override
        public int hashCode() {
            return Objects.hash(src, dst);
        }
    }
}