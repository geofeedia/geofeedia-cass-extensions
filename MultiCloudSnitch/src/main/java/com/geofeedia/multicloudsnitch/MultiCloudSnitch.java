package com.geofeedia.multicloudsnitch;

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
import java.util.HashMap;
import java.util.Map;


public class MultiCloudSnitch extends AbstractNetworkTopologySnitch {
    protected static final Logger LOG = LoggerFactory.getLogger(MultiCloudSnitch.class);
    private static final String GCE_ZONE_QUERY_URL = "http://metadata.google.internal/computeMetadata/v1/instance/zone";
    private static final String AWS_ZONE_QUERY_URL = "http://169.254.169.254/latest/meta-data/placement/availability-zone";
    private static final String DEFAULT_DATACENTER = "UNKNOWN-DC";
    private static final String DEFAULT_RACK = "UNKNOWN-RACK";
    private Map<InetAddress, Map<String, String>> savedEndpoints;
    protected String zone;
    protected String region;


    public MultiCloudSnitch() throws IOException, ConfigurationException {
        SnitchProperties props = new SnitchProperties();
        String datacenter = props.get("dc", "");
        String response;
        Map<String, String> headers = new HashMap<>();

        if (datacenter.isEmpty()) {
            throw new ConfigurationException("No datacenter provided in cassandra-rackdc.properties file");
        }

        // check for aws prefix for provider
        if (datacenter.matches("aws-(.*)")) {
            // get instance metadata
            response = getMetadataApiCall(AWS_ZONE_QUERY_URL, null);

            // Split "us-central1-a" or "asia-east1-a" into "us-central1"/"a" and "asia-east1"/"a".
            String[] splits = response.split("-");
            zone = splits[splits.length - 1];

            // hack/fix for CASSANDRA-4026
            region = response.substring(0, response.length() - 1);
            if (region.endsWith("1")) {
                region = response.substring(0, response.length() - 3);
            }

            String dataCenterSuffix = props.get("dc_suffix", "");
            region = region.concat(dataCenterSuffix);
            LOG.info("MultiCloudSnitch in AWS using region: {}, zone: {}", region, zone);

        // check for gce prefix for provider
        } else if (datacenter.matches("gce-(.*)")) {
            headers.put("Metadata-Flavor", "Google");
            // get instance metadata
            response = getMetadataApiCall(GCE_ZONE_QUERY_URL, headers);

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
            region = region.concat(dataCenterSuffix);
            LOG.info("MultiCloudSnitch in GCE using region: {}, zone: {}", region, zone);

        } else {
            throw new ConfigurationException("Unable to identify datacenter provider. Currently support GCE and AWS");
        }
    }

    public String getMetadataApiCall(String url, Map<String, String> headers) throws IOException, ConfigurationException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        DataInputStream dataInputStream = null;

        try {
            conn.setRequestMethod("GET");

            // set any request headers if present
            if (headers != null) {
                for (Map.Entry<String, String> header : headers.entrySet()) {
                    conn.setRequestProperty(header.getKey(), header.getValue());
                }
            }

            if (conn.getResponseCode() != 200) {
                throw new ConfigurationException("Unable to execute API call at the following url: " + url);
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
        if (state == null || state.getApplicationState(ApplicationState.RACK) == null)
        {
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
        if (state == null || state.getApplicationState(ApplicationState.DC) == null)
        {
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
