package com.mode.trino.honeycomb;

import com.google.auto.service.AutoService;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Collections;

@AutoService(Plugin.class)
public class HoneycombPlugin implements Plugin {
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        this.getEventListenerFactories();

        return Collections.singleton(new HoneycombConnectorFactory());
    }
}
