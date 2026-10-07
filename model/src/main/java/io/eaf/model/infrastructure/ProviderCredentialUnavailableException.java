package io.eaf.model.infrastructure;

import java.io.IOException;

final class ProviderCredentialUnavailableException extends IOException {
    ProviderCredentialUnavailableException(String message) { super(message); }
}
