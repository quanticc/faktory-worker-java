package com.github.quanticc.faktory;

import java.util.Objects;
import java.util.concurrent.FutureTask;

public class FutureFaktoryTask {

    private final FaktoryJob faktoryJob;
    private final FaktoryTask faktoryTask;
    private final FutureTask<Void> future;

    public FutureFaktoryTask(FaktoryJob faktoryJob, FaktoryTask faktoryTask, FutureTask<Void> future) {
        this.faktoryJob = faktoryJob;
        this.faktoryTask = faktoryTask;
        this.future = future;
    }

    public FaktoryJob getFaktoryJob() {
        return faktoryJob;
    }

    public FaktoryTask getFaktoryTask() {
        return faktoryTask;
    }

    public FutureTask<Void> getFuture() {
        return future;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FutureFaktoryTask that = (FutureFaktoryTask) o;
        return Objects.equals(faktoryJob, that.faktoryJob) &&
                Objects.equals(faktoryTask, that.faktoryTask) &&
                Objects.equals(future, that.future);
    }

    @Override
    public int hashCode() {
        return Objects.hash(faktoryJob, faktoryTask, future);
    }
}
