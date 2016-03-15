# MultiCloudSnitch

[Cassandra snitch](https://docs.datastax.com/en/cassandra/2.0/cassandra/architecture/architectureSnitchesAbout_c.html) for cross-cloud configuration.

Currently supports Google Cloud and AWS.

#### Example
If in GCE and a `dc_suffix=_item` then the datacenter would be `gce-us-east1_item` and
the rack would be `a` if we were in that zone.

If in AWS and a `dc_suffix=_item` then the datacenter would be `aws-us-east-1_item` and
the rack would be `a` if we were in that zone.