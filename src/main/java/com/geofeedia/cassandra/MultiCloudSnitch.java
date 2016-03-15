package com.geofeedia.cassandra;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.AbstractNetworkTopologySnitch;
import org.apache.cassandra.locator.SnitchProperties;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;


public class MultiCloudSnitch extends AbstractNetworkTopologySnitch {
    protected static final Logger LOG = LoggerFactory.getLogger(MultiCloudSnitch.class);
    private static final String GCE_ZONE_QUERY_URL = "http://169.254.169.254/computeMetadata/v1/instance/zone";
    private static final String AWS_ZONE_QUERY_URL = "http://169.254.169.254/latest/meta-data/placement/availability-zone";
    private static final String DEFAULT_DATACENTER = "UNKNOWN-DC";
    private static final String DEFAULT_RACK = "UNKNOWN-RACK";
    private Map<InetAddress, Map<String, String>> savedEndpoints;
    protected String zone;
    protected String region;


    public MultiCloudSnitch() throws IOException, ConfigurationException {
        SnitchProperties props = new SnitchProperties();
        String response = this.getMetaData();

        // if no response the neither URL returned metadata so unable to continue.
        if (response == null) {
            throw new ConfigurationException("Unable to determine cloud. Checked both GCE and AWS with the following urls: [" +
                    GCE_ZONE_QUERY_URL + ", " + AWS_ZONE_QUERY_URL + "]");
        }

        // check for aws prefix for provider
        if (response.matches("aws_(.*)")) {
            // remove cloud prefix (aws_)
            response = response.substring(4);

            zone = response.substring(response.length() - 1);

            // hack/fix for CASSANDRA-4026
            region = response.substring(0, response.length() - 1);
            if (region.endsWith("1")) {
                region = response.substring(0, response.length() - 1);
            }

            String dataCenterSuffix = props.get("dc_suffix", "");
            StringBuilder builder = new StringBuilder();
            // build the final region string.
            // should be in the format of <cloud_provider>-<region><dc_suffix>
            region = builder.append("aws-").append(region).append(dataCenterSuffix).toString();
            LOG.info("MultiCloudSnitch in AWS using region: {}, zone: {}", region, zone);

        // if not in aws then check for gce
        } else if (response.matches("gce_(.*)")) {
            // remove cloud prefix (gce_)
            response = response.substring(4);

            // since google returns something like `projects/736271449687/zones/us-east1-b`
            // we want to split on the slashes and only get the last section for the az.
            String[] splits = response.split("/");
            String az = splits[splits.length - 1];

            // Split "us-central1-a" or "asia-east1-a" into "us-central1"/"a" and "asia-east1"/"a".
            splits = az.split("-");
            zone = splits[splits.length - 1];
            int lastRegionIdx = az.lastIndexOf("-");
            region = az.substring(0, lastRegionIdx);

            String dataCenterSuffix = props.get("dc_suffix", "");
            StringBuilder builder = new StringBuilder();
            // build the final region string.
            // should be in the format of <cloud_provider>-<region><dc_suffix>
            region = builder.append("gce-").append(region).append(dataCenterSuffix).toString();
            LOG.info("MultiCloudSnitch in GCE using region: {}, zone: {}", region, zone);

        // unknown cloud (i.e. not GCE or AWS) prefix so throw exception
        } else {
            throw new ConfigurationException("There was an error getting the region and zone from either GCE or AWS");
        }
    }

    private String getMetaData() throws IOException, ConfigurationException {
        // dynamically determine which cloud we are in (gce/aws)
        // by hitting the end points.
        String metaData = awsApiCall();
        if (metaData != null) {
            // append aws to indicate we are in aws
            LOG.info("Getting metadata from AWS.");
            return "aws_" + metaData;
        }

        metaData = gceApiCall();
        if (metaData != null) {
            // append gce to indicate we are in gce
            LOG.info("Getting metadata from GCE.");
            return "gce_" + metaData;
        }

        return null;
    }

    private String awsApiCall() throws IOException, ConfigurationException {
        HttpURLConnection conn = (HttpURLConnection) new URL(AWS_ZONE_QUERY_URL).openConnection();
        DataInputStream dataInputStream = null;

        try {
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200) {
                return null;
            }

            int contentLength = conn.getContentLength();
            byte[] bytes = new byte[contentLength];
            dataInputStream = new DataInputStream((FilterInputStream) conn.getContent());
            return new String(bytes, StandardCharsets.UTF_8);

        } finally {
            FileUtils.close(dataInputStream);
            conn.disconnect();
        }
    }

    private String gceApiCall() throws IOException, ConfigurationException {
        HttpURLConnection conn = (HttpURLConnection) new URL(GCE_ZONE_QUERY_URL).openConnection();
        DataInputStream dataInputStream = null;

        try {
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Metadata-Flavor", "Google");
            if (conn.getResponseCode() != 200) {
                return null;
            }

            int contentLength = conn.getContentLength();
            byte[] bytes = new byte[contentLength];
            dataInputStream = new DataInputStream((FilterInputStream) conn.getContent());
            return new String(bytes, StandardCharsets.UTF_8);

        } finally {
            FileUtils.close(dataInputStream);
            conn.disconnect();
        }
    }

    public String getRack(InetAddress endpoint) {
        if (endpoint.equals(FBUtilities.getBroadcastAddress())) {
            return zone;
        }

        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null || state.getApplicationState(ApplicationState.RACK) == null) {
            if (savedEndpoints == null) {
                savedEndpoints = SystemKeyspace.loadDcRackInfo();
            }
            if (savedEndpoints.containsKey(endpoint)) {
                return savedEndpoints.get(endpoint).get("rack");
            }
            return DEFAULT_RACK;
        }
        return state.getApplicationState(ApplicationState.RACK).value;
    }

    public String getDatacenter(InetAddress endpoint) {
        if (endpoint.equals(FBUtilities.getBroadcastAddress())) {
            return region;
        }

        EndpointState state = Gossiper.instance.getEndpointStateForEndpoint(endpoint);
        if (state == null || state.getApplicationState(ApplicationState.DC) == null) {
            if (savedEndpoints == null) {
                savedEndpoints = SystemKeyspace.loadDcRackInfo();
            }
            if (savedEndpoints.containsKey(endpoint)) {
                return savedEndpoints.get(endpoint).get("data_center");
            }
            return DEFAULT_DATACENTER;
        }
        return state.getApplicationState(ApplicationState.DC).value;
    }
}
