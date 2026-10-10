package io.eaf.model.infrastructure;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

final class ProviderFailureSupport {
    private ProviderFailureSupport() { }

    static boolean capacityExceeded(Throwable failure) {
        return causedBy(failure, ProviderCapacityExceededException.class) != null;
    }

    static boolean quotaUnavailable(Throwable failure) {
        return causedBy(failure, ProviderQuotaUnavailableException.class) != null;
    }

    static <T extends Throwable> T causedBy(Throwable failure, Class<T> type) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        for (var cause = failure; cause != null && seen.add(cause); cause = cause.getCause())
            if (type.isInstance(cause)) return type.cast(cause);
        return null;
    }

    static String describe(Throwable failure) {
        Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        var description = new StringBuilder();
        for (var cause = failure; cause != null && seen.add(cause); cause = cause.getCause())
            description.append(' ').append(cause.getClass().getName()).append(' ')
                    .append(String.valueOf(cause.getMessage()));
        return description.toString();
    }
}
