package com.mode.trino.honeycomb;

import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class HoneycombConnectorMetadata implements ConnectorMetadata {
    private static final Logger LOGGER =
        Logger.getLogger(HoneycombConnectorMetadata.class.getCanonicalName());

    private String catalogName;
    private final HoneycombClient client;

    public HoneycombConnectorMetadata(String catalogName, HoneycombClient client) {
        this.catalogName = catalogName;
        this.client = client;
    }

    @Override
    public boolean schemaExists(ConnectorSession session, String schemaName) {
        boolean exists = "datasets".equalsIgnoreCase(schemaName);
        LOGGER.info("Checking if schema exists: schemaName = " + schemaName + "; exists = " + exists);
        return exists;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        try {
            throw new Exception("give me a stack trace");
        } catch (Exception e) {
            LOGGER.log(Level.INFO, "Listing schema names", e);
        }
        return List.of("datasets");
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        LOGGER.info("Getting table handle: tableName = " + tableName);
        return new HoneycombTableHandle(client, tableName);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        return ((HoneycombTableHandle) table).getTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        LOGGER.info("Listing tables: schemaName = " + schemaName.orElse("<empty>"));
        try {
            return client.getAllDatasets()
                .execute()
                .body()
                .stream()
                .map(dataset -> new SchemaTableName("datasets", dataset.getSlug()))
                .toList();
        } catch (IOException e) {
            throw new TrinoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public Stream<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        LOGGER.info("Streaming table columns: prefix = " + prefix);
        String tablePrefixOrName = prefix.getTable().orElse("*");

        if (tablePrefixOrName.contains("*")) {
            String regularExpression = "^" + tablePrefixOrName.replace("*", ".*") + "$";
            Pattern pattern = Pattern.compile(regularExpression);
            return listTables(session, Optional.of("datasets"))
                .stream()
                .filter(name -> pattern.matcher(name.getTableName()).matches())
                .map(name -> getTableColumnsMetadataForDataset(name.getTableName()));
        } else {
            return Stream.of(getTableColumnsMetadataForDataset(tablePrefixOrName));
        }
    }

    private TableColumnsMetadata getTableColumnsMetadataForDataset(String datasetName) {
        LOGGER.info("Getting table columns metadata for dataset: datasetName = " + datasetName);
        try {
            List<ColumnMetadata> columns = client.getDatasetAttributes(datasetName)
                .execute()
                .body()
                .stream()
                .map(attribute -> new ColumnMetadata(attribute.getKeyName(), honeycombTypeToTrinoType(attribute.getType())))
                .toList();
            return TableColumnsMetadata.forTable(new SchemaTableName("datasets", datasetName), columns);
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
