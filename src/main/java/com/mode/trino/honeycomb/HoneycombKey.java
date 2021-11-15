package com.mode.trino.honeycomb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HoneycombKey {
    private final Map<String, Boolean> apiKeyAccess;
    private final HoneycombTeam team;

    @JsonCreator
    public HoneycombKey(@JsonProperty("api_key_access") Map<String, Boolean> apiKeyAccess,
                        @JsonProperty("team") HoneycombTeam team) {
        this.apiKeyAccess = apiKeyAccess;
        this.team = team;
    }

    public Map<String, Boolean> getApiKeyAccess() {
        return apiKeyAccess;
    }

    public HoneycombTeam getTeam() {
        return team;
    }

    @Override
    public String toString() {
        return "HoneycombKeyDetails{" +
            "apiKeyAccess=" + apiKeyAccess +
            ", team=" + team +
            '}';
    }
}
