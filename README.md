# Graduation-Project
Graduation project and the paper presented in [BalkanCom 2019](http://www.balkancom.info/2019/sessions.html#students). The paper can be download in [here](https://github.com/bubblecounter/Graduation-Project/blob/master/Performance%20Evaluation%20of%20Load%20Balancing%20Algorithms%20in%20Leaf-Spine%20Based%20Data%20Center%20Networks.pdf) for more detailed information about the project and its results.

# Abstract
  Traffic in Data Center Networks (DCN) has increased drastically over the years. With the emergence of Software Defined Networking (SDN), the information gathered about the network has increased significantly. Apart from SDN, a solution proposed in DCNs to decrease the latency and complexity is using the leaf spine topology. This topology has taken its place in DCNs that experience east-west traffic rather than the north-south traffic, and the need for high availability for information in DCNs has led the research for accurate and efficient load balancing methods. In this paper, the performance of six different load balancing algorithms, namely random, Round Robin, Least Flow, Least Utilization, Proactive and Laberio, are evaluated and compared for DCN with leaf spine topology, using ONOS as the SDN controller for the first time in the literature. The performance metrics utilized include Round Trip Time (RTT) standard deviation of RTT, instantaneous and average throughput. It has been observed that Least Flow has the smallest average RTT and smallest standard deviation of RTT among the load balancing algorithms. In terms of throughput, Proactive and Laberio give the best results, and among all, Laberio is the only method that responds to the changes which occur after the installation of flow rules to the switches.
## Keywords
load balancing, leaf-spine topology, Software Defined Networking (SDN), ONOS, Data Center Networks (DCN)
## Simulation Setup
  For the simulations, the leaf-spine topology depicted in figure below has been created in [mininet](http://mininet.org/), an emulator tool for SDN-based networks. The topology consists of two layers; two spine switches on first layer and four-leaf switches on second layer. In the simulations with the Mininet tool, the switches are implemented as OpenFlow switches and the SDN controller running is [ONOS](https://www.opennetworking.org/onos/). The load balancing algorithms to be compared are implemented as ONOS applications.
#### Topology
![Topology](https://github.com/bubblecounter/Graduation-Project/blob/master/topology.png "Topology")

## Traffic Scenario
  As the traffic scenario for evaluating the load balancing algorithms, we have considered two hosts for each leaf spine switch and one of the hosts (h1), acting as a TCP server, while others are acting as TCP clients.  The simulation scenario involves the events listed on Table 1. h1 is the TCP server. Hosts h8, h6, h7 start to send TCP packets of 2962 bytes to server consecutively, with 30 seconds in between. Then, at the 90th second, h6 stops its flow to create an imbalance in the network. 30 seconds after that, h5, h4, h3 starts to send TCP packets of 2962 bytes, in order, with 30 second intervals. At the end of 240 seconds the simulation is ended. The packets are sent and received via Iperf, which is a tool used to create TCP clients and server at hosts. With the help of [wireshark](https://www.wireshark.org/) network analyzer tool, we have captured packets at the server h1 and TCP client h8. Then from the captured packets we have observed the RTT and throughput measurements for each load balancing algorithm.
 
#### Timeline
| Time(sec)     | Event         |
|:------------- |:------------- | 
| 0	  | h8 starts sending TCP packets to h1 |
| 30	| h6 starts sending TCP packets to h1 |
| 60	| h7 starts sending TCP packets to h1 |
| 90	| h6 stops sending TCP packets to h1  |
| 120	| h6 starts sending TCP packets to h1 |
| 150	| h5 starts sending TCP packets to h1 |
| 180	| h4 starts sending TCP packets to h1 |
| 210	| h3 starts sending TCP packets to h1 |
| 240	| The end of Simulation

## Performance Metrics
  RTT, jtter, average and instantaneous throughput

## Conclusion
  In this project, we have evaluated and compared four reactive a proactive and a dynamic load balancing method which are random, round robin, least flow, least utilization, proactive and Laberio algorithms for a leaf-spine DCN. Realistic simulations (emulations) are performed on the leaf-spine topology using ONOS as SDN controller. The RTT and throughput metrics are observed. The results indicate that Least Flow has the smallest jitter. It has been found that Proactive and Laberio has the highest throughput and Laberio is the only method which responds to load imbalances emerged after the path selection. 
  In the light of these results it can be said that for networks which require stable and small RTT it is better to use Least Flow load balancing method. Another outcome is that Laberio and Proactive methods suit better to the networks which require higher throughput, at the cost of higher CPU usage. For networks which face highly varying flows, Laberio performs best because of its dynamic characteristics.

## How to install?
### ONOS
  The tutorial in the [link](https://wiki.onosproject.org/display/ONOS/Development+Environment+Setup) can be followed to install ONOS.
### mininet
> git clone git://github.com/mininet/mininet <br/>
cd mininet<br/>
git tag  # list available versions<br/>
git checkout -b 2.2.1 2.2.1  # or whatever version you wish to install<br/>
cd ..<br/>

 Once you have the source tree, the command to install Mininet is:
> mininet/util/install.sh<br/>
 
 For more detailed [info](http://mininet.org/download/)
### wireshark
> sudo apt-get install libcap2-bin wireshark<br/>
### Algorithms
  Algorithms implemented as ONOS applications. You can download them from [here](https://github.com/bubblecounter/Graduation-Project/tree/master/Algorithms). After downlading them you need to copy them to *onos* folder in order to run. There are 6 algorithms in total which are Random(onos's default forwarding app), Round Robin, Least flow, Least utilization, Laberio and Proactive. The origin of the proactive app is taken from [here](https://git.fslab.de/mmklab/ONOS_based_Load- Balancing_in_SDWMNs) and made some improvements.

## How to run?
**Run Onos**
>cd onos<br/>
bazel run onos-local -- clean debug<br/>

**open onos terminal to activate apps**
> tools/test/bin/onos localhost<br/>

**You can enter the link below to your browser to open ONOS GUI**
  localhost:8181/ui/login.html

**Run mininet**

> sudo -i <br/> cd mininet/mininet/examples <br/> sudo python leaf_spine.py 2 4 127.0.0.1   
  
or alternatively you can open your predefined topology using miniedit
> ./miniedit.py<br/>

**Installing the ONOS Applications(Algorithms)**
> cd onos/apps/{$installed app's folder} <br/>
mvn clean install -DskipTests <br/>
onos-app localhost install target/{$installed app's name}.oar<br/>

**Activate installed app**
Open onos's terminal and execute the following command

> app activate org.onosproject.fwd <br/>

**Observing performance metrics**
Open terminals for hosts in mininet using the example command below
> xterm h1<br/>

Then, you can run wireshark on hosts via terminals
Finally, start packet transmission between hosts and retrieve performance metrics using wireshark.
