/*
 * Copyright 2017-present Open Networking Laboratory
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
package apps.hbrs.fwd.src.main.java.org.hbrs.fwd;

import com.google.common.collect.ImmutableSet;
import org.onosproject.net.HostId;
import org.onosproject.net.flow.FlowEntry;
import org.onosproject.net.flow.instructions.Instruction;
import org.onosproject.net.flow.instructions.Instructions;
import org.onosproject.net.link.LinkEvent;
import org.onosproject.net.link.LinkListener;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.onlab.packet.MacAddress;
import org.onosproject.app.ApplicationAdminService;
import org.onosproject.app.ApplicationService;
import org.onosproject.cfg.ComponentConfigService;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Host;
import org.onosproject.net.Link;
import org.onosproject.net.Path;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.config.NetworkConfigEvent;
import org.onosproject.net.config.NetworkConfigListener;
import org.onosproject.net.config.NetworkConfigRegistry;
import org.onosproject.net.config.NetworkConfigService;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.device.PortStatistics;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleEvent;
import org.onosproject.net.flow.FlowRuleListener;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;
import org.onosproject.net.flow.criteria.Criterion;
import org.onosproject.net.flow.criteria.EthCriterion;
import org.onosproject.net.flowobjective.DefaultForwardingObjective;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.ForwardingObjective;
import org.onosproject.net.host.HostEvent;
import org.onosproject.net.host.HostListener;
import org.onosproject.net.host.HostService;
import org.onosproject.net.link.LinkService;
import org.onosproject.net.packet.PacketService;
import org.onosproject.net.statistic.FlowStatisticService;
import org.onosproject.net.topology.TopologyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Date;
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
import static org.onosproject.net.flow.FlowRuleEvent.Type.RULE_REMOVED;
import static org.onosproject.net.flow.criteria.Criterion.Type.ETH_DST;
import static org.onosproject.net.flow.criteria.Criterion.Type.ETH_SRC;


/**
 * <h1>Proactive Load-Balancing ONOS application</h1>
 * @author Moritz Schlebusch @HBRS
 * @since 2017
 * @version 2.0
 */
@Component(immediate = true)
public class hbrsfwd {
    private final Logger log = LoggerFactory.getLogger(getClass());
    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected TopologyService topologyService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected PacketService packetService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowStatisticService flowStatisticService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected NetworkConfigRegistry configRegistry;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected NetworkConfigService configService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ApplicationService applicationService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ApplicationAdminService applicationAdminService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected DeviceService deviceService;


    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected ComponentConfigService componentConfigService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected HostService hostService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected LinkService linkService;


    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowRuleService flowRuleService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected FlowObjectiveService flowObjectiveService;


    private ApplicationId appId;
    private ApplicationId onosforwarding;
    private List<Host> hostList = new ArrayList<>();

    private final NetworkConfigListener configListener = new InternalConfigListener();
    private final LocalDeviceListener deviceListener = new LocalDeviceListener();
    private final LocalHostListener hostListener = new LocalHostListener();
    private final LocalFlowListener flowListener = new LocalFlowListener();
    private final LocalLinkListener linkListener = new LocalLinkListener();

    private int DEFAULT_RULE_PRIO = 50000;
    private final int LOCAL_RULE_PRIO = 60000;
    private final int DEFAULT_RULE_TIMEOUT = 20 ;
    private final int LOCAL_RULE_TIMEOUT = 600;
    // This determines the minium time between executions of  the global path optimization procedure

    private final int PATH_REFRESH_TIME = 10;

    private Date lastExecutionTimedeployShortestPath = null;
    private final executionSynchronisation runDeployShortestPath = new executionSynchronisation();
    // internal variable to track the state of the app.
    // if true, the app is shutting down.
    private boolean disable_service = false;
    // Decide if weather or not to stop the onos forwarding app to begin of this app.
    // (Will be started again after exiting this app)
    private final boolean deactivate_onos_app = true;

    private static final int DEFAULT_TEST_SCENARIO = 4;
/*    @Property(name = "test_scenario", intValue = DEFAULT_TEST_SCENARIO,
            label = "The evaluation test-scenario (1,2,3 or 4)")*/


    // Test-Scenario (App I => scenario nr. 2 and APP II => scenario nr. 4 //
    // Test-scenarios:
    // 1> Hopcount only
    // 2> Flowcount + Hopcount
    // 3> UsagePercent + FlowCount + Hopcount
    // 4> Cap - Load + Flowcount + Hopcount
    private final int test_scenario = 4;

    private ExecutorService blackHoleExecutor;


    @Activate
    protected void activate() {
        String appname = "org.hbrs.fwd.app";
        appId = coreService.registerApplication(appname);
        disable_service = false;
        onosforwarding = coreService.getAppId("org.onosproject.fwd");

        if (deactivate_onos_app) {
            /* deactivate org.onos.fwd, only if it is appropriate */
            try {
                applicationAdminService.deactivate(onosforwarding);
                log.info("### Deactivating Onos Reactive Forwarding App ###");
            } catch (NullPointerException ne) {
                log.info(ne.getMessage());
            }
        }

        log.info("### Started " + appId.name() + " with id " + appId.id() + " ###");

        deviceService.addListener(deviceListener);
        hostService.addListener(hostListener);
        flowRuleService.addListener(flowListener);
        linkService.addListener(linkListener);

        // Set PortStat-Polling Frequency to one time per second. (To get Bytes per Second)
        log.info("### Setting portStatsPollFrequency to 1s ###");
        componentConfigService.setProperty("org.onosproject.provider.of.device.impl.OpenFlowDeviceProvider", "portStatsPollFrequency", "1");

        log.info("Log-Name: "+ log.getName());
        createLocalRules();
        deployShortestPath();
        blackHoleExecutor = newSingleThreadExecutor(groupedThreads("onos/app/hbrs.fwd",
                                                                   "black-hole-fixer",
                                                                   log));


    }
    @Deactivate
    protected void deactivate() {
        /* activate org.onos.fwd, only if it was disabled in activate method */
        if (deactivate_onos_app) {
            try {
                applicationAdminService.activate(onosforwarding);
                log.info("### Activating Onos Reactive Forwarding App ###");
            } catch (NullPointerException ne) {
                log.info(ne.getMessage());
            }

            disable_service = true;
            flowRuleService.removeFlowRulesById(appId);
            blackHoleExecutor.shutdown();
            blackHoleExecutor = null;
            log.info("### Stopped and removed flowrules. ###");

        }

    }

    /**
     * FlowRuleExists
     * Searchs for existing flow-rules, returns
     * @param hostA
     * @param hostB
     * @return true - if flow already exists, false if not
     */
    public boolean flowRuleExists(Host hostA, Host hostB)
    {
        // This block searches for already exiting flow rules for this host-pair
        for (FlowRule flowRule : flowRuleService.getFlowEntriesById(appId))
        {
            Criterion criterion = flowRule.selector().getCriterion(ETH_SRC);
            MacAddress src= null;
            MacAddress dst = null;

            if (criterion != null)
                src = ((EthCriterion) criterion).mac();
            criterion = flowRule.selector().getCriterion(ETH_DST);
            if (criterion != null)
                dst = ((EthCriterion) criterion).mac();


            if(dst != null && src != null) {
                if (hostA.mac().toString().matches(src.toString()) && hostB.mac().toString().matches(dst.toString())){
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
     * Host-based optimal pathfinder
     * Deploy FlowRules for Flow (A to B,B to A)
     * This is the a central method, which makes use of the ONOS services and the bestPath() method.
     * It's function is a Host-based optimal pathfinder.
     * @param hostA Host A-Source
     * @param hostB Host B-Destination
     */
    public void deployShortestPath(Host hostA, Host hostB){

        if (flowRuleExists(hostA, hostB))
        {
            log.info("### Flow already exists! Not deploying Flow-Rules for Host-combo ("+hostA.mac().toString()+":"+hostB.mac().toString()+") again! ");
            return;
        }

        log.info("Evaluating paths for Host-combo ("+hostA.mac().toString()+":"+hostB.mac().toString()+")");
        Set<Path> ps = topologyService.getPaths(topologyService.currentTopology(), hostA.location().deviceId(), hostB.location().deviceId());
        log.info("Path-size, by normal service:: " +ps.size());

        Stream<Path> pstream = topologyService.getKShortestPaths(topologyService.currentTopology(), hostA.location().deviceId(), hostB.location().deviceId());

        //Convert a Stream to Set
        Set<Path> pathSet = pstream.collect(Collectors.toSet());

        log.info("K-Path-size: " + pathSet.size());

        Path bestPath = bestPathInSet(pathSet);

        if (bestPath == null)   log.info("### I could not find a path between  " + hostA.id()+ " and " + hostB.id() + " ###");
        else
        {
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
     * Host-based optimal pathfinder (scans + connects all Hosts in the system)
     * This is the a central method, which makes use of the ONOS services and the bestPath() method.
     * It's function is a Host-based optimal pathfinder.
     *
     *
     */
    public void deployShortestPath() {

        if (runDeployShortestPath.runNow(PATH_REFRESH_TIME)) {

            log.info("### Optimal pathfinder starts. (Global) ###");
            log.info("### Hostcount: " + hostService.getHostCount() + " ###");
            /* Querying known Host-Systems from the controller **/
            try {
                TimeUnit.SECONDS.sleep(2);
                log.info("### Waiting 2 seconds, to find more hosts ###");
            } catch (InterruptedException e) {
                log.info(e.getLocalizedMessage());
                e.printStackTrace();
            }
            log.info("### New Hostcount: " + hostService.getHostCount() + " ###");

            /* Querying Known Hosts from ONOS hostsevice **/
            Iterable<Host> hostIterable = hostService.getHosts();
            /* Converting Iterable to ArrayList **/
            List<Host> hostList = new ArrayList<>();
            hostIterable.forEach(hostList::add);

            /* Iterate through all possible combinations of hosts, find paths between hosts, find best paths, deploy paths **/
            if (hostList.size() > 1)
                for (int a = 0; a < hostList.size(); a++) {
                    for (int b = 0; b < hostList.size(); b++) {
                        /* exclude A==B combinations **/
                        if (a != b) {
                            Host hostA = hostList.get(a);
                            Host hostB = hostList.get(b);
                            DeviceId switchA = hostA.location().deviceId();
                            DeviceId switchB = hostB.location().deviceId();

                            /*  exclude Host-AB-pairs connected to the same switch **/
                            if (switchA.toString().equals(switchB.toString())) {
                                // do nothing, if hostA and hostB are connected to the same switch
                            } else {
                                deployShortestPath(hostA, hostB);
                            }

                        }
                    }
                }
            else
                log.info("### There is only " + hostList.size() + " hosts, nothing to do. ###");
        }
    }


    /**
     * Host-based optimal pathfinder (scans + connects all Hosts in the system)
     * This is the a central method, which makes use of the ONOS services and the bestPath() method.
     * It's function is a Host-based optimal pathfinder.
     * @param host The host to deploy the paths for
     *
     *
     */
    public void deployShortestPath(Host host) {

        log.info("### Starting Pathfinder for Host: "+host.ipAddresses()+" ###");
        log.info("### Hostcount: " + hostService.getHostCount() + " ###");

        /* Querying Known Hosts from ONOS hostsevice **/
        Iterable<Host> hostIterable = hostService.getHosts();
        /* Converting Iterable to ArrayList **/
        List<Host> hostList = new ArrayList<>();
        hostIterable.forEach(hostList::add);

        /* Iterate through all hosts, find paths between hosts, find best paths, deploy paths **/
        if (hostList.size() > 1)
            for (int a = 0; a < hostList.size(); a++) {

                Host hostA = hostList.get(a);
                Host hostB = host;

                DeviceId switchA = hostA.location().deviceId();
                DeviceId switchB = hostB.location().deviceId();

                /* exclude A==B combinations & ...**/
                /*  exclude Host-AB-pairs connected to the same switch **/
                if (switchA.toString().equals(switchB.toString())) {

                    // do nothing, if hostA and hostB are connected to the same switch
                } else {
                    // Deploy FlowRules for Flow (A->B,B->A)
                    deployShortestPath(hostA, hostB);
                }
            }
        else
            log.info("### There is only " + hostList.size() + " hosts, nothing to do. ###");
    }


    /**
     * setFLows deploys all the flow-rules at the switches along a linklist
     * Deploy FlowRules for Flow (A to B, B to A)
     * @param linkList A list of those link-objects to deploy rules for
     * @param srcMac The Flow Source Device
     * @param dstMac The Flow Destination Device
     */
    public void setFLows(List<Link> linkList, MacAddress srcMac, MacAddress dstMac){
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
     * @param switchDevice ID of Switch to deploy to
     * @param srcMac The Flow Source Device
     * @param dstMac The Flow Destination Device
     * @param outPort The Flow Forwarding Out:Port
     */
    public void setFlow(DeviceId switchDevice, MacAddress srcMac, MacAddress dstMac, PortNumber outPort){
        setFlow(switchDevice, srcMac, dstMac, outPort, DEFAULT_RULE_TIMEOUT);
    }

    /**
     * overloads original setFlow with default priority parameter
     * @param switchDevice ID of Switch to deploy to
     * @param srcMac The Flow Source Device
     * @param dstMac The Flow Destination Device
     * @param outPort The Flow Forwarding Out:Port
     * @param timeout The Flow-(Soft)-Timeout
     */
    public void setFlow(DeviceId switchDevice, MacAddress srcMac, MacAddress dstMac, PortNumber outPort, int timeout){
        setFlow(switchDevice, srcMac, dstMac, outPort, timeout, DEFAULT_RULE_PRIO);
    }

    /**
     * Helper Method, pushs FlowRules to Switching Devices
     * @param switchDevice ID of Switch to deploy to
     * @param srcMac The Flow Source Device
     * @param dstMac The Flow Destination Device
     * @param outPort The Flow Forwarding Out:Port
     * @param timeout The Flow-(Soft)-Timeout
     * @param flowPriority The Flow Priority
     */
    public void setFlow(DeviceId switchDevice, MacAddress srcMac, MacAddress dstMac, PortNumber outPort, int timeout, int flowPriority){

        /*  Define which packets(flows) to handle **/
        TrafficSelector.Builder selectorBuilder = DefaultTrafficSelector.builder();
        /*  if no src-Mac is defined forward disregarding source address (used in local switch rules) */
        if (srcMac != null){
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
        if (srcMac == null) log.info("### Add Flow at Switch "+ switchDevice.toString() + " dst: " + dstMac.toString() + " outPort: " + outPort.toString()+ " ###");
        if (srcMac != null) log.info("### Add Flow at Switch "+ switchDevice.toString() + " src: "+ srcMac.toString() + " dst: " + dstMac.toString() + " outPort: " + outPort.toString()+ " ###");
        flowObjectiveService.forward(switchDevice, forwardingObjective);
    }


    /**
     * Querys a List of Hosts from Onos-Hostservice, Iterates through test List, pushs FlowRules to the Switching devices they are connected to
     *
     */
    public void createLocalRules(){
        /* Querying known Host-Systems from the controller **/
        Iterator<Host> hostIterator = hostService.getHosts().iterator();

        while (hostIterator.hasNext()){
            Host hostA = hostIterator.next();
            log.info("### Setting local Flows for Host: " + hostA.mac().toString() + " ###");
            setFlow(hostA.location().deviceId(), null, hostA.mac(), hostA.location().port(), LOCAL_RULE_TIMEOUT, LOCAL_RULE_PRIO);
        }
    }

    /**
     * Iterate through a given List of Hosts,push FlowRules to the Switching devices they are connected to
     * @param hostIterator List of Hosts, to define local rules for.
     */
    public void createLocalRules(Iterator<Host> hostIterator){
        while (hostIterator.hasNext()){
            Host hostA = hostIterator.next();
            log.info("### Setting local Flows for Host: " + hostA.mac().toString() + " ###");
            setFlow(hostA.location().deviceId(), null, hostA.mac(), hostA.location().port(), LOCAL_RULE_TIMEOUT, LOCAL_RULE_PRIO);
        }
    }

    /**
     * Push "local" FlowRules to the Switching device to provide connectivity
     * @param host Host, for which deploy a local rule for.
     */
    public void createLocalRules(Host host){
        log.info("### Setting local Flows for Host: " + host.mac().toString() + " ###");
        setFlow(host.location().deviceId(), null, host.mac(), host.location().port(), LOCAL_RULE_TIMEOUT, LOCAL_RULE_PRIO);
    }

    /**
     * Find hosts connected to given Device and push FlowRules to the Switching device to provide connectivity
     * @param device Device, on which to deploy local rule for directly connected hosts for.
     */
    public void createLocalRules(DeviceId device){
        Set<Host> hosts = hostService.getConnectedHosts(device);
        Iterator<Host> hostIterator = hosts.iterator();

        /* Iterate through all hosts, directly connected to the switch **/
        while (hostIterator.hasNext()){
            Host hostA = hostIterator.next();
            log.info("### Setting local Flows for Host: " + hostA.mac().toString() + " ###");
            setFlow(hostA.location().deviceId(), null, hostA.mac(), hostA.location().port(), LOCAL_RULE_TIMEOUT, LOCAL_RULE_PRIO);
        }
    }

    /**
     * loadOnLink
     * Querys the Receive-Rate of the Ingress-Port of the second hop in the link
     * @param link the link
     * @return the receive rate of the Ingress-Port of the second hop in the link
     */
    public double loadOnLink(Link link)
    {
        DeviceId dst_id = link.dst().deviceId();
        DeviceId src_id = link.src().deviceId();
        Port dst_port = deviceService.getPort(link.dst());
        Port src_port = deviceService.getPort(link.src());

        PortStatistics srcPortStats = deviceService.getDeltaStatisticsForPort(src_id, src_port.number());
        PortStatistics dstPortStats = deviceService.getDeltaStatisticsForPort(dst_id, dst_port.number());

        float rcvRate = ((dstPortStats.bytesReceived() * 8));
        long rcvPktDrops = dstPortStats.packetsRxDropped();
        float sntRate = ((srcPortStats.bytesSent() * 8) );
        long sntPktDrops = srcPortStats.packetsTxDropped();

        return rcvRate;
    }

    /**
     * maxUsagePercentOnPath
     * Returns the highest usage percent on the path
     * @param path
     * @return the highest usage percent on the path
     */
    public double maxUsagePercentOnPath(Path path)
    {
        double maxUsage = 0;
        for(Link link : path.links())
        {
            double load = loadOnLink(link);
            double capacity = capacityOfLink(link);
            double usagePercent;
            if (capacity >0)    usagePercent = (load * 100 / capacity );
            else                usagePercent = 100;

            maxUsage=usagePercent;
        }
        return maxUsage;
    }

    /**
     * minFreeOnPath
     * Return the smallest free capacity on the path
     * @param path
     * @return the smallest free capacity on the path
     */
    public double minFreeOnPath(Path path)
    {
        double minFree = -7411.0;
        for(Link link : path.links())
        {
            double load = loadOnLink(link);
            double capacity = capacityOfLink(link);
            // Brutto-Capacity to expected Netto-Capacity.
            double nettoCapacity = bruttoToNetto(capacity);
            double free = capacity - load;//double free = nettoCapacity - load;
            log.info("brutto-cap: " + capacity);
            log.info("netto-cap: " + nettoCapacity);
            log.info("load: " + load);
            // If first run: Set minfree = free
            if(minFree == -7411.0) minFree = free;
            // If cap-load is smaller than 0> set to 0.
            if(free<0)
            {
                log.info("There is more Load("+load+") than Capacity("+nettoCapacity+")");
                free=0;
            }
            // if this free is smaller than the previously smallest, set minfree = free.
            if (free < minFree)
            {
                minFree = free;
            }
        }
        return minFree;
    }

    /**
     * Count the Flow's, currently deployed on this path. Only Flows matching Devices and outports are counted.
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

                    } catch (NullPointerException npe) { log.info("NullPointerException: " + npe.getLocalizedMessage());
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
     * @param paths Set of paths, to find best one in.
     * @return The least loaded path
     */
    public Path bestPathInSet(Set<Path> paths)
    {
        log.info("Starting now");
        // If the Set is empty, return null
        int count_paths = paths.size();

        if (count_paths==0) return null;
        Path bestPath = paths.iterator().next();

        if (count_paths==1) return bestPath;
        double[] maxLinkUsage = new double[count_paths];
        double[] minFreeonPath = new double[count_paths];
        int[] hopcount = new int[count_paths];
        int[] numberOfFlows = new int[count_paths];
        Path[] pathList = new Path[count_paths];

        int i =0;
        for ( Path path : paths)
        {
            pathList[i]  = path;
            maxLinkUsage[i] = maxUsagePercentOnPath(path); //=> The  usage percent of the link with the highest usage on the path.
            minFreeonPath[i] = minFreeOnPath(path); // => The free capacity of the link with the least free capacity on the path.
            hopcount[i] = (int)path.cost(); // => the path-hopcount metric
            numberOfFlows[i] = numberOfFlowsOnPath(path); // => The number of flows set on this path.
            log.info("Path-Log: No. " + i + ", nexthop: " + path.links().iterator().next().dst().deviceId() + ", minFree: "+minFreeonPath[i]+", maxusage: "+maxLinkUsage[i]+ ", NumberOfFLows: "+numberOfFlows[i]+", hopcount: "+hopcount[i]);
            i++;
        }


        //Different evaluation scenarios:
        //1: Only Hopcount
        //2: Hopcount + Flowcount
        //3: Hopcount + Flowcount + UsagePercentage

        if(test_scenario == 1)
        {
            log.info("Path decision based on scenario 1: Only Hopcount is used !!!");
            /** PATH DECISION: compare hopcount ***/
            if (smallestNumberFromArray(hopcount) == -1) {
                // -1 => All Paths have the same amount of hops.
                log.info("thesis-log: Paths are all equal: Nexthop " + bestPath.links().get(0).dst().deviceId());
                return bestPath;
            } else // NumberOfFlowRules are equal, but hopcount is different:
            {
                log.info("hopcount is different.");
                int index = smallestNumberFromArray(hopcount);
                bestPath = pathList[index];
                return bestPath;
            }
        }

        if(test_scenario == 2)
        {
            log.info("Path decision based on scenario 2: Flowcount + Hopcount is used !!!");
            /** PATH DECISION: 2. compare amount of Flows ***/
            if (smallestNumberFromArray(numberOfFlows) == -1)
            {
                // -1 => All Paths have the same amount of Flows
                /** PATH DECISION: 3. compare hopcount ***/
                if (smallestNumberFromArray(hopcount) == -1) {
                    // -1 => All Paths have the same amount of hops.
                    log.info("### Pathes are all equal,(FlowRules:" + numberOfFlows[0] + ", hc:" + hopcount[0] + ") choosing the first one ###");
                    log.info("thesis-log: Paths are all equal. Choosing:" + bestPath.toString());
                    return bestPath;
                } else // NumberOfFlowRules are equal, but hopcount is different:
                {
                    int index = smallestNumberFromArray(hopcount);
                    bestPath = pathList[index];
                    log.info("thesis-log: Paths have the same amount of flows: " + numberOfFlows[0] + ", but hopcount differs. Choosing:" + bestPath.toString());
                    return bestPath;
                }
            } else // NumberOfFlows differs:
            {
                int index = smallestNumberFromArray(numberOfFlows);
                bestPath = pathList[index];
                log.info("thesis-log: Decision based on Flow-Count: Choosing the path with least flows(" + numberOfFlows[index] + "). Path: "+bestPath.toString());
                return bestPath;
            }
        }

        if(test_scenario == 3)
        {
            log.info("Path decision based on scenario 3: Using Usages, Flowcount and Hopcount !!!");
            /** PATH DECISION: 1. compare usage-percent ***/
            /* Use a helper method to get the index of the less-loaded path in the Load-Array */
            if (smallestUsagePercentFromArray(maxLinkUsage) == -1) {
                // -1 => All Paths have equal LinkUsages.
                /** PATH DECISION: 2. compare amount of Flows ***/
                if (smallestNumberFromArray(numberOfFlows) == -1) {
                    // -1 => All Paths have the same amount of Flows
                    /** PATH DECISION: 3. compare hopcount ***/
                    if (smallestNumberFromArray(hopcount) == -1) {
                        // -1 => All Paths have the same amount of hops.
                        log.info("thesis-log: Paths are all equal,(usage:" + maxLinkUsage[0] + ", FlowRules:" + numberOfFlows[0] + ", hc:" + hopcount[0] + ") choosing the first one: " + bestPath.toString());
                        return bestPath;
                    } else // All UsagePercents and NumberOfFlowRules are equal, but hopcount is different:
                    {
                        int index = smallestNumberFromArray(hopcount);
                        bestPath = pathList[index];
                        log.info("thesis-log: All UsagePercents are similar: " + maxLinkUsage[0] + ", all paths have the same amount of flows: " + numberOfFlows[0] + ", but hopcount is differs. Path: " + bestPath.toString());
                        return bestPath;
                    }
                } else // All UsagePercents are equal, but NumberOfFlows differs:
                {
                    int index = smallestNumberFromArray(numberOfFlows);
                    log.info("thesis-log: All UsagePercents are similar: " + maxLinkUsage[0] + ". Choosing the path with least flows(" + numberOfFlows[index] + ")");
                    bestPath = pathList[index];
                    return bestPath;
                }
            } else // UsagePercent differs
            {
                int index = smallestUsagePercentFromArray(maxLinkUsage);
                log.info("thesis-log: Choosing path with the lowest UsagePercent(" + maxLinkUsage[index] + ")");
                bestPath = pathList[index];
                return bestPath;
            }
        }

        if(test_scenario == 4)
        {
            log.info("Path decision based on scenario 4: Using (Capacity-Load), Flowcount and Hopcount !!!");
            /** PATH DECISION: 1. compare usage-percent ***/
            /* Use a helper method to get the index of the less-loaded path in the Load-Array */
            if (biggestMinFreeOnPath(minFreeonPath) == -1) {
                // -1 => All Paths have equal LinkUsages.
                /** PATH DECISION: 2. compare amount of Flows ***/
                if (smallestNumberFromArray(numberOfFlows) == -1) {
                    // -1 => All Paths have the same amount of Flows
                    /** PATH DECISION: 3. compare hopcount ***/
                    if (smallestNumberFromArray(hopcount) == -1) {
                        // -1 => All Paths have the same amount of hops.
                        log.info("thesis-log: Paths are similar, Free capacity: " + minFreeonPath[0] + ", FlowRules:" + numberOfFlows[0] + ", hc:" + hopcount[0] + ") choosing the first one: " + bestPath.toString());
                        return bestPath;
                    } else // All UsagePercents and NumberOfFlowRules are equal, but hopcount is different:
                    {
                        int index = smallestNumberFromArray(hopcount);
                        bestPath = pathList[index];
                        log.info("thesis-log: Paths are similar, Free capacity: " + minFreeonPath[0] + ", all paths have the same amount of flows: " + numberOfFlows[0] + ", but hopcount is differs. Choosing: " + bestPath.toString());
                        return bestPath;
                    }
                } else // All UsagePercents are equal, but NumberOfFlows differs:
                {
                    int index = smallestNumberFromArray(numberOfFlows);
                    bestPath = pathList[index];
                    log.info("thesis-log: Paths are similar, Free capacity: " + minFreeonPath[0] + ". Choosing the path with least flows(" + numberOfFlows[index] + ") Bestpath: " +bestPath.toString());
                    return bestPath;
                }
            } else // minFree differs
            {
                int index = biggestMinFreeOnPath(minFreeonPath);
                bestPath = pathList[index];
                log.info("thesis-log: Choosing path with the highest unused capacity!(" + minFreeonPath[index] + ") Bestpath: " + bestPath.toString());
                return bestPath;
            }
        }

        return bestPath;
    }



    /**
     * Find the smallest array-field and return the index. If all Fields are the same, return -1.
     * @param array The Path-Array
     * @return the array-index of the smallest field
     */
    public static int smallestNumberFromArray(int [] array){
        OptionalInt max = IntStream.of(array).max();
        OptionalInt min = IntStream.of(array).min();
        int min_id = IntStream.of(array).boxed().collect(toList()).indexOf(min.getAsInt());
        // IF the smallest int is equal with the biggest one, return -1, else return index of the smallest one.
        if(max.getAsInt() == min.getAsInt()) return -1;
        else return min_id;
    }

    /**
     * Calculate the approximate netto-capacity on a link, given the brutto data-rate, respectively its modulation mode.
     * @param capacity The brutto capacity
     * @return expected netto-capacity
     */
    public double bruttoToNetto(double capacity){
        double netto;
        double x = 1;
        /* The following x -factors are based on measurements in each topology and with each data-rate **/
        /* Its goal is to get a capacity value, that is as close as possible at the according load-value reported by the port-stats **/
        switch((int) capacity){
            case 6000000:   x = 1.9;
                break;
            case 12000000:  x = 1.625;
                break;
            case 24000000:  x = 1.333;
                break;
            case 36000000:  x = 1.16;
                break;
            case 54000000:  x = 0.943;
                break;
        }
        netto = capacity / 100 * x;
        return netto;
    }

    /**
     * Returns the Index of the entry with the smallest Usage-Percent. If all entrys are similar (delta 5 %) return -1.
     * @param array (UsagePercentArray)
     * @return  the index of the smallest usagepercent field
     */
    public int smallestUsagePercentFromArray (double [] array){
        OptionalDouble max = DoubleStream.of(array).max();
        OptionalDouble min = DoubleStream.of(array).min();
        int min_id = DoubleStream.of(array).boxed().collect(toList()).indexOf(min.getAsDouble());
        // If the smallest and the biggest values are more close as 5,  return -1, else return index of the smallest one.
        log.info("## UsagePercent comparison: " + max.getAsDouble() + " vs. " + min.getAsDouble());
        if( Math.abs(max.getAsDouble() - min.getAsDouble()) < 0.05 ) return -1;
        else return min_id;
    }

    /**
     * biggestFreeeOnPath
     * returns the index of the field with the  highest free capacity value
     * @param array - the array to search in
     * @return the index of the field with the  highest free capacity value
     */
    public int biggestMinFreeOnPath(double [] array){
        // first sort, than find min and max. => Choose max.
        OptionalDouble max = DoubleStream.of(array).max();
        OptionalDouble min = DoubleStream.of(array).min();

        int max_id = DoubleStream.of(array).boxed().collect(toList()).indexOf(max.getAsDouble());
        // If min = max, than return -1 (All paths are equal)
        log.info("Max Free = " + max.getAsDouble() + ", Min_Free= "+ min.getAsDouble());
        // 2000 is approx 3%. Which is used as saturation of a link is not always the same.
        if( Math.abs(max.getAsDouble() - min.getAsDouble()) < 2000 ) return -1;
        //otherwise return path id with max free
        return max_id;
    }

    /**
     * As ONOS does not know the Mac-Address of a Switch-Port, This method re
     * The mac-addressing scheme is the same on the mininet/ns3 site
     * @param switchPort (ConnectionPoint)
     * @return The belonging MAC-Address, empty String of Mac unknown.
     */
    public String macaddressOfSwitchPort(ConnectPoint switchPort){
        long port = switchPort.port().toLong();
        String switchID = switchPort.deviceId().toString();
        switch (switchID){
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
        log.info( "!!! There is no Mac-Address known for the sw/port combo: " + switchID +  " " + port );
        return "";
    }

    /**
     * capacityOfLink
     * Reads the current link-capacity from a file that is constantly updated by ns-3 (capacity-patch)
     * @param link  - the concerned link
     * @return the current link capacity
     */
    public int capacityOfLink(Link link){
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
        if (rate == 33000000) log.info("It seems like there was no datarate information for this mac.");
        else{
            log.info("Got capacity rate: " + rate);
        }
        return rate;
    }




    // Only Listeners beyond from here //
    // Listens for our removed flows.
    private class LocalFlowListener implements FlowRuleListener {
        /**
         * FlowRuleEvent-handler; Re-deploys rules if rules time-out.
         *
         * @param event The Flow-Rule event
         */
        @Override
        public void event(FlowRuleEvent event) {
            if (event.subject() != null) {
                FlowRule flowRule = event.subject();
                MacAddress src = null;
                MacAddress dst = null;
                Criterion criterion = null;

                /* Flow-Timeout-Method */
                if (event.type() == RULE_REMOVED && flowRule.appId() == appId.id()) {
                    // If App is "shutting down" don't do anything.
                    if (disable_service) {
                        //App is shutting down, do not react on the flow remove
                        return;
                    }
                    if (flowRule != null)
                        criterion = flowRule.selector().getCriterion(ETH_SRC);
                    if (criterion != null)
                        src = ((EthCriterion) criterion).mac();
                    criterion = flowRule.selector().getCriterion(ETH_DST);
                    if (criterion != null)
                        dst = ((EthCriterion) criterion).mac();

                    int priority = flowRule.priority();

                    if (priority == LOCAL_RULE_PRIO) {
                        log.info("### Local rule timed out at switch " + flowRule.deviceId() + ", dst: " + dst + " ###");
                        log.info("### Restarting local rule generator ###");

                        if (hostService.getHostsByMac(dst).size() > 0)
                            createLocalRules(hostService.getHostsByMac(dst).iterator().next());
                        else
                            log.info("### Host " + dst + " is gone, not going to renew local flow rule ###");
                    } else {
                        log.info("### Flowrule timed out: " + flowRule.deviceId() + ", src: " + src + ", dst: " + dst + ", prio: " + priority + " ###");
                        log.info("### Restarting Host-based optimal pathfinder ###");

                        /* Only Deploy New Flows if both hosts still exist! */
                        if(hostService == null) log.info("!!! Hostservice down, ONOS needs restart !!!");

                        else {
                            if (hostService.getHostsByMac(src) != null && hostService.getHostsByMac(dst) != null)
                                if (hostService.getHostsByMac(src).size() > 0 && hostService.getHostsByMac(dst).size() > 0)
                                    deployShortestPath(hostService.getHostsByMac(src).iterator().next(), hostService.getHostsByMac(dst).iterator().next());
                                else
                                    log.info("### One of the Hosts is not present anymore ###");
                            else
                                log.info("### One of the Hosts is not present anymore ###");
                        }
                    }
                }
            }
        }
    }
    private class LocalHostListener implements HostListener {
        @Override
        public void event(HostEvent event) {
            switch (event.type()) {
                case HOST_ADDED:

                    /* Querying known Host-Systems from the controller **/
                    try {
                        Host newHost = event.subject();
                        log.info("### HOSTEVENT, new Host: " + newHost.mac().toString() + " ###");
                        log.info("### Trying to create local rules & new Host2Host flows ###");

                        /* Restarting Local Rules Process for the Switch(ID) where the host is located **/
                        createLocalRules(newHost);
                        /* Restarting the HOST2HOST Path method **/
                        deployShortestPath(newHost);

                    }catch(Exception e){
                        log.info("Exception while New-Host Event");
                        log.info(e.getLocalizedMessage());}

                    break;
                case HOST_REMOVED:

                    break;
                default:
                    break;
            }
        }
    }

    private class LocalLinkListener implements LinkListener {
        @Override
        public void event(LinkEvent event) {
            switch (event.type()) {
                case LINK_REMOVED:
                    if (blackHoleExecutor != null) {
                        blackHoleExecutor.submit(() -> fixBlackhole(event.subject().src()));
                    }
                    break;
                case LINK_ADDED:

                    break;
                default:
                    break;
            }
        }
    }

    private class LocalDeviceListener implements DeviceListener {

        @Override
        public void event(DeviceEvent event) {
            switch (event.type()) {
                case DEVICE_AVAILABILITY_CHANGED:
                    Device device = event.subject();
                    if(device != null)
                        log.info("Device " + device.id() + " disconnected, re-deploying flows.");
                    try {
                        flowRuleService.purgeFlowRules(device.id());
                    }
                    catch(NullPointerException np){
                        log.info("NullPointer, because device already gone. " + np.getMessage());
                    }

                    break;
                case DEVICE_ADDED:
                    log.info("Here is a new Switch: "  + event.subject().toString());
                    break;
                case DEVICE_REMOVED:
                    log.info("Switch removed : "  + event.subject().toString());

                    break;
                case DEVICE_SUSPENDED:

                    break;
                case PORT_REMOVED:

                    break;
                case PORT_ADDED:

                    break;
                default:
            }
        }
    }


    private class InternalConfigListener implements NetworkConfigListener {
        @Override
        public void event(NetworkConfigEvent event) {

            switch (event.type()) {
                case CONFIG_ADDED:
                    log.info("Network configuration added");

                    break;
                case CONFIG_UPDATED:
                    log.info("Network configuration updated");

                    break;
                default:
                    break;
            }
        }
    }

    private class executionSynchronisation{
        Date lastExecutionTime;
        public executionSynchronisation(){
            lastExecutionTime = null;
        }

        public boolean runNow(long refreshTime){
            Date now = new Date();
            //init with refreshtime, so in case of first run it will return true
            long diff = refreshTime;
            if (lastExecutionTime != null) diff = ( now.getTime() - lastExecutionTime.getTime()) / 1000;

            if( diff >= refreshTime ) {
                /* In case of first execution or last execution was long enough ago: Set Date and go on **/
                lastExecutionTime = new Date();
                return true;
            }
            else return false;

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
    // Wrapper class for a source and destination pair of MAC addresses
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
