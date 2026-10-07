package io.eaf.model.infrastructure;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

final class ProviderFailureSupport {
    private ProviderFailureSupport() { }

    static boolean capacityExceeded(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (var cause = failure; cause != null && seen.add(cause); cause = cause.getCause())
            if (cause instanceof ProviderCapacityExceededException) return true;
        return false;
    }

    static boolean quotaUnavailable(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (var cause = failure; cause != null && seen.add(cause); cause = cause.getCause())
            if (cause instanceof ProviderQuotaUnavailableException) return true;
        return false;
    }
}
