package com.github.quanticc.faktory;

import com.fasterxml.jackson.annotation.JsonProperty;

public class FaktoryHandshake {

    @JsonProperty("v")
    private int version;
    @JsonProperty("s")
    private String nonce;
    @JsonProperty("i")
    private int iterations;

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public String getNonce() {
        return nonce;
    }

    public void setNonce(String nonce) {
        this.nonce = nonce;
    }

    public int getIterations() {
        return iterations;
    }

    public void setIterations(int iterations) {
        this.iterations = iterations;
    }
}
