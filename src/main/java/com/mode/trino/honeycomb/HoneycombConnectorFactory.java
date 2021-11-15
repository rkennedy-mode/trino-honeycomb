package com.mode.trino.honeycomb;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;
import java.util.Map;
import java.util.Optional;

public class HoneycombConnectorFactory implements ConnectorFactory {
    private static final String API_KEY_PROPERTY = "api.key";
    private static final String API_URL_PROPERTY = "api.url";

    @Override
    public String getName() {
        return "honeycomb";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver() {
        return new HoneycombHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context) {
        HoneycombClient client = HoneycombClient.retrofit(config.get(API_KEY_PROPERTY),
            Optional.ofNullable(config.get(API_URL_PROPERTY)));
        return new HoneycombConnector(catalogName, client);
    }
}
