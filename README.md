# Graduation-Project
Graduation project and the paper presented in BalkanCom 2019.
# Abstract
Traffic in Data Center Networks (DCN) has increased drastically over the years. With the emergence of Software Defined Networking (SDN), the information gathered about the network has increased significantly. Apart from SDN, a solution proposed in DCNs to decrease the latency and complexity is using the leaf spine topology. This topology has taken its place in DCNs that experience east-west traffic rather than the north-south traffic, and the need for high availability for information in DCNs has led the research for accurate and efficient load balancing methods. In this paper, the performance of six different load balancing algorithms, namely random, Round Robin, Least Flow, Least Utilization, Proactive and Laberio, are evaluated and compared for DCN with leaf spine topology, using ONOS as the SDN controller for the first time in the literature. The performance metrics utilized include Round Trip Time (RTT) standard deviation of RTT, instantaneous and average throughput. It has been observed that Least Flow has the smallest average RTT and smallest standard deviation of RTT among the load balancing algorithms. In terms of throughput, Proactive and Laberio give the best results, and among all, Laberio is the only method that responds to the changes which occur after the installation of flow rules to the switches.
## Keywords
load balancing, leaf-spine topology, Software Defined Networking (SDN), ONOS, Data Center Networks (DCN)

## Used Technologies
[ONOS](https://www.opennetworking.org/onos/) as SDN Controller


[mininet](http://mininet.org/) emulator for SDN-enabled networks
[wireshark](https://www.wireshark.org/) to observe performance metrics
