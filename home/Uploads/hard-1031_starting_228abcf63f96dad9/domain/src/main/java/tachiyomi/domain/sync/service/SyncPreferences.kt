package tachiyomi.domain.sync.service

import tachiyomi.core.common.preference.Preference
import tachiyomi.core.common.preference.PreferenceStore

class SyncPreferences(
    preferenceStore: PreferenceStore,
) {

    /** Master switch of the delta sync engine. */
    val deltaSyncEnabled: Preference<Boolean> = preferenceStore.getBoolean("delta_sync_enabled", true)

    /**
     * Base URL of the sync endpoint. Deltas are POSTed to `<base>/push` and fetched from `<base>/pull`.
     * Defaults to a mock server on the host machine of the Android emulator.
     */
    val deltaSyncEndpoint: Preference<String> = preferenceStore.getString(
        "delta_sync_endpoint",
        DEFAULT_ENDPOINT,
    )

    /** Opaque position in the remote change feed; everything up to it has been applied. */
    val deltaSyncPullCursor: Preference<String> = preferenceStore.getString(
        Preference.appStateKey("delta_sync_pull_cursor"),
        "",
    )

    private val deviceIdPreference: Preference<String> = preferenceStore.getString(
        Preference.appStateKey("delta_sync_device_id"),
        "",
    )

    /** Stable random id of this installation, lets the server avoid echoing our own changes back. */
    fun deviceId(): String {
        return deviceIdPreference.get().ifEmpty {
            java.util.UUID.randomUUID().toString().also(deviceIdPreference::set)
        }
    }

    companion object {
        const val DEFAULT_ENDPOINT = "http://10.0.2.2:8080/mock-sync/v1"
    }
}
