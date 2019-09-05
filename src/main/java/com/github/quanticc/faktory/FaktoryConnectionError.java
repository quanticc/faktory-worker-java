package com.github.quanticc.faktory;

public class FaktoryConnectionError extends Exception {
    public FaktoryConnectionError(String response) {
        super(response);
    }
}
