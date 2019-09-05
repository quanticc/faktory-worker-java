package com.github.quanticc.faktory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;

import java.util.*;

@JsonDeserialize(builder = FaktoryJob.Builder.class)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FaktoryJob {

    @JsonProperty("jid")
    private final String jobId;
    @JsonProperty("jobtype")
    private final String jobType;
    private final List<Object> args;
    private final Map<String, Object> custom;

    private FaktoryJob(String jobId, String jobType, List<Object> args, Map<String, Object> custom) {
        this.jobId = Objects.requireNonNull(jobId);
        this.jobType = Objects.requireNonNull(jobType);
        this.args = args == null ? Collections.emptyList() : args;
        this.custom = custom;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobType() {
        return jobType;
    }

    public List<Object> getArgs() {
        return args;
    }

    public Map<String, Object> getCustom() {
        return custom;
    }

    @JsonPOJOBuilder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Builder {

        @JsonProperty("jid")
        private String jobId;
        @JsonProperty("jobtype")
        private String jobType;
        private List<Object> args = new ArrayList<>();
        private Map<String, Object> custom;

        public Builder withJobId(String jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder withJobType(String jobType) {
            this.jobType = jobType;
            return this;
        }

        public Builder withArgs(List<Object> args) {
            this.args = args;
            return this;
        }

        public Builder withCustom(Map<String, Object> custom) {
            this.custom = custom;
            return this;
        }

        public Builder putCustom(String key, Object value) {
            if (this.custom == null) {
                this.custom = new LinkedHashMap<>();
            }
            this.custom.put(key, value);
            return this;
        }

        public FaktoryJob build() {
            return new FaktoryJob(jobId, jobType, args, custom);
        }

    }
}
