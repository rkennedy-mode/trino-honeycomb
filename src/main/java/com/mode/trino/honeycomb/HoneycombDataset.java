package com.mode.trino.honeycomb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class HoneycombDataset {
    private final String name;
    private final String slug;

    @JsonCreator
    public HoneycombDataset(@JsonProperty("name") String name,
                            @JsonProperty("slug") String slug) {
        this.name = name;
        this.slug = slug;
    }

    public String getName() {
        return name;
    }

    public String getSlug() {
        return slug;
    }

    @Override
    public String toString() {
        return "HoneycombDataset{" +
            "name='" + name + '\'' +
            ", slug='" + slug + '\'' +
            '}';
    }
}
