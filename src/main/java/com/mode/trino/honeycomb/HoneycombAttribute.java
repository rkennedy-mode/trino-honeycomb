package com.mode.trino.honeycomb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HoneycombAttribute {
    private final String id;
    private final String keyName;
    private final boolean hidden;
    private final String description;
    private final String type;

    @JsonCreator
    public HoneycombAttribute(@JsonProperty("id") String id,
                              @JsonProperty("key_name") String keyName,
                              @JsonProperty("hidden") boolean hidden,
                              @JsonProperty("description") String description,
                              @JsonProperty("type") String type) {
        this.id = id;
        this.keyName = keyName;
        this.hidden = hidden;
        this.description = description;
        this.type = type;
    }

    public String getId() {
        return id;
    }

    public String getKeyName() {
        return keyName;
    }

    public boolean isHidden() {
        return hidden;
    }

    public String getDescription() {
        return description;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return "HoneycombAttribute{" +
            "id='" + id + '\'' +
            ", keyName='" + keyName + '\'' +
            ", hidden=" + hidden +
            ", description='" + description + '\'' +
            ", type='" + type + '\'' +
            '}';
    }
}
