package com.github.quanticc.faktory;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class FaktoryTask {

    private final String jobType;
    private final Consumer<FaktoryJob> action;

    public FaktoryTask(String jobType, Consumer<FaktoryJob> action) {
        this.jobType = jobType;
        this.action = action;
    }

    public String getJobType() {
        return jobType;
    }

    public Consumer<FaktoryJob> getAction() {
        return action;
    }

    @Override
    public String toString() {
        return "FaktoryTask{" +
                "jobType='" + jobType + '\'' +
                '}';
    }
}
