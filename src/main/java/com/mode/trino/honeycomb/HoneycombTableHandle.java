package com.mode.trino.honeycomb;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.logging.Logger;

public class HoneycombTableHandle implements ConnectorTableHandle {
    private static final Logger LOGGER = Logger.getLogger(HoneycombTableHandle.class.getCanonicalName());

    private final HoneycombClient client;
    private final SchemaTableName tableName;

    public HoneycombTableHandle(HoneycombClient client, SchemaTableName tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    public ConnectorTableMetadata getTableMetadata() {
        return new ConnectorTableMetadata(tableName, getTableColumnsMetadataForDataset());
    }

    private List<ColumnMetadata> getTableColumnsMetadataForDataset() {
        try {
            return client.getDatasetAttributes(tableName.getTableName())
                .execute()
                .body()
                .stream()
                .map(attribute -> ColumnMetadata.builder()
                    .setName(attribute.getKeyName())
                    .setType(honeycombTypeToTrinoType(attribute.getType()))
                    .setComment(Optional.ofNullable(attribute.getDescription()))
                    .setNullable(true)
                    .build())
                .toList();
        } catch (IOException e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
        }
    }

    private Type honeycombTypeToTrinoType(String type) {
        return switch (type) {
            case "float" -> DoubleType.DOUBLE;
            case "boolean" -> BooleanType.BOOLEAN;
            case "integer" -> BigintType.BIGINT;
            default -> VarcharType.createUnboundedVarcharType();
        };
    }
}
