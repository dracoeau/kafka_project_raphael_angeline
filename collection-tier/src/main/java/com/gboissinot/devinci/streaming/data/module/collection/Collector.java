package com.gboissinot.devinci.streaming.data.module.collection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.SSLContext;
import java.util.stream.IntStream;

class Collector {

    //private static final String OPEN_DATA_URL = "https://opendata.paris.fr//api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&rows=1400&facet=overflowactivation&facet=creditcard&facet=kioskstate&facet=station_state";
	private static final String OPEN_DATA_URL = "https://opendata.paris.fr/api/records/1.0/search/?dataset=prises_electriques_vehicules_tps_reel&facet=static_name&facet=plugs_outletmodel&facet=plugs_status&facet=plugs_orientation&facet=static_brand&facet=static_owner&facet=geolocation_postalcode&facet=geolocation_city";
    private static final Logger logger = LoggerFactory.getLogger(Collector.class);

    private final KafkaPublisher publisher;

    Collector(KafkaPublisher publisher) {
        this.publisher = publisher;
    }

    void collect() {
        try {

            TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
            SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
            SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
                    NoopHostnameVerifier.INSTANCE);

            Registry<ConnectionSocketFactory> socketFactoryRegistry =
                    RegistryBuilder.<ConnectionSocketFactory>create()
                            .register("https", sslsf)
                            .register("http", new PlainConnectionSocketFactory())
                            .build();

            BasicHttpClientConnectionManager connectionManager =
                    new BasicHttpClientConnectionManager(socketFactoryRegistry);
            CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(sslsf)
                    .setConnectionManager(connectionManager).build();

            HttpComponentsClientHttpRequestFactory requestFactory =
                    new HttpComponentsClientHttpRequestFactory(httpClient);

            ResponseEntity<String> response =
                    new RestTemplate(requestFactory).exchange(OPEN_DATA_URL, HttpMethod.GET, null, String.class);

            String jsonString = response.getBody();

            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonNode = mapper.readTree(jsonString);
            JsonNode records = jsonNode.get("records");

            IntStream.range(0, 1400).forEach(value -> {
                JsonNode currentRecordNode = records.get(value);
                if (currentRecordNode != null) {
                    publisher.publish(currentRecordNode.toString());
                }
            });

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}