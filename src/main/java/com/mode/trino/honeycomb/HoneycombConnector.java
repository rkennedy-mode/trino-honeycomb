package com.mode.trino.honeycomb;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

public class HoneycombConnector implements Connector {
    private static final ConnectorTransactionHandle TRANSACTION_HANDLE = new ConnectorTransactionHandle() {};
    private String catalogName;
    private final HoneycombClient honeycombClient;

    public HoneycombConnector(String catalogName, HoneycombClient honeycombClient) {
        this.catalogName = catalogName;
        this.honeycombClient = honeycombClient;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly) {
        return TRANSACTION_HANDLE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle) {
        return new LoggingConnectorMetadata(new HoneycombConnectorMetadata(catalogName, honeycombClient));
    }
}
