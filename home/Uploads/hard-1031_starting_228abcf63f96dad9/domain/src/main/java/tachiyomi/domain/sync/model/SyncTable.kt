package tachiyomi.domain.sync.model

/**
 * Tables whose rows are tracked by the delta sync engine. [tableName] is the value stored in
 * `sync_delta_queue.table_name`.
 */
enum class SyncTable(val tableName: String) {
    MANGAS("mangas"),
    CHAPTERS("chapters"),
    ;

    companion object {
        fun fromTableName(name: String): SyncTable? = entries.firstOrNull { it.tableName == name }
    }
}
