# MultiCloudSnitch

[Cassandra snitch](https://docs.datastax.com/en/cassandra/2.0/cassandra/architecture/architectureSnitchesAbout_c.html) for cross-cloud configuration.

Currently supports Google Cloud and AWS.

Relies on the datacenter configuration in `cassandra-rackdc.properties` file. In particular the `dc` property
by prefixing with the provider (i.e. gce/aws) in the form of `<cloud_provider_here>-<region>-<zone>`.

#### example `cassandra-rackdc.properties`
```yaml
# These properties are used with GossipingPropertyFileSnitch and will
# indicate the rack and dc for this node
dc=gce-us-east1
rack=b

# Add a suffix to a datacenter name. Used by the Ec2Snitch and Ec2MultiRegionSnitch
# to append a string to the EC2 region name.
#dc_suffix=

# Uncomment the following line to make this snitch prefer the internal ip when possible, as the Ec2MultiRegionSnitch does.
# prefer_local=true
```