# Overview

[HPCC Systems,](http://HPCCSystems.com) an open source High Performance Computing Cluster, is a massive parallel-processing computing platform that solves Big Data problems. HPCC Systems is an enterprise-proven platform for manipulating, transforming, querying, and data warehousing Big Data. Built by LexisNexis, the HPCC platform has helped it grow to a $1.5 billion information solutions company.

The HPCC Systems architecture incorporates a data query engine (called Thor) and a data delivery engine (called Roxie), as well as common middleware support components, an external communications layer, client interfaces which provide both end-user services and system management tools, and auxiliary components to support monitoring and to facilitate loading and storing of file system data from external sources.

An HPCC environment can include only Thor clusters, or both Thor and Roxie clusters. The HPCC Juju charm creates a cluster which contains both, but you can customize it after deployment.

See [How it Works](http://www.hpccsystems.com/Why-HPCC/How-it-works)  for more details. 

See [System Requirements](http://hpccsystems.com/download/docs/system-requirements) for  hardware details. 

The HPCC Juju Charm encapsulates best practice configurations for the HPCC  Systems Platform.  You can use a Juju Charm to stand up an HPCC Platform on:

- Local Provider (LXC)

- Amazon Web Services Cloud


# Usage

## General Usage

1. To deploy an HPCC Cluster:

    `juju deploy hpcc <cluster_name>`

    **For example:**

        'juju deploy hpcc cluster1`

1. To check the status , run
        juju status

        You also can log into the node to check if HPCC is properly installed.

        `juju ssh cluster1/0`

Once the service is deployed and running, you can find the address for the GUI by running juju status and looking for the public-address field for the juju-gui service

1.  Once HPCC is properly installed, you can add more nodes using this command:

        `juju add-unit <cluster_name> -n <#_of_nodes_to_add>`

    **For example:**

        `juju add-unit cluster1 -n 3`

1. You can expose the HPCC cluster by running:

       `juju expose <cluster_name>`

Once the service is deployed, running, and exposed, you can find the address for the ECL Watch Web interface by running juju status and looking for the public-address field. Type that address plus :8010 for the port.

For example, **nnn.nnn.nnn.nnn:8010**.

If you have multiple nodes, the ECL Watch node will be the lowest IP address (first hpcc node listed).



# Configuration

When you use the `juju add-unit` command to add nodes, scripts are called automatically to provide a default configuration. 

If you want to configure manually, set **auto-gen** to **0**, wait for all nodes to be in a "started" state, then call the **config_hpcc.sh**  script using the following parameters:

`./config_hpcc.sh -thornodes <# of thor nodes> -roxienodes <# of roxie nodes> -supportnodes <# of support nodes> -slavespernode <#of thor slaves per node> 
`

Another useful script reports the URL for the ECL Watch node. Call the **get-url.sh** script to display the cluster configuration and the URL for the ECL Watch service.

### ssh-keys ###
The hpcc charm automatically generates a key pair  (*id\_rsa*  &  *id\_rsa.pub*) to configure nodes. 

If you already have your own key pair and wish to use it, copy and paste their contents into the two variables (*ssh-key-public* and *ssh-key-private*) in the configuration file (config.yaml) or in the Juju canvas configuration settings.  

### Verifying the checksum
The charm uses an md5sum to verify the checksum of the HPCC platform  package before installing.  

For this version of the charm, it is set to check the md5sum for the Community Edition Version 4.2.0-4 for Ubuntu 12.04. To verify a different version, edition, or OS version, change the value of the md5sum in the package-checksum variable in config.yaml. 

 

### AWS Cloud

When deploying to Amazon Web Services Cloud, the charm automatically opens for external access, the following ports:

- Port **8010** for ECLWatch access
- Port **8002** for WsECL access.
- Port **9876** for direct Roxie access
- Port **8015** for Configuration Manager access.  

### Next Steps ###

After deploying and adding nodes, you can tweak various options to optimize your HPCC deployment to meet your needs.
 
See [HPCC Systems Web site](http://HPCCSystems.com) for more details.


# HPCC Systems Contact Information

[HPCC Systems Web site](http://HPCCSystems.com)

For support, visit the HPCC Community Forums:
[HPCC Community Forums](http://hpccsystems.com/bb/index.php?sid=0bda2dddb2ea50418357171d33b11e5f)
