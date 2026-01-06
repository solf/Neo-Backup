/*
 * Neo Backup: open-source apps backup and restore app.
 * Copyright (C) 2025  Antonios Hazim
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.machiav3lli.backup.manager.handler

import com.machiav3lli.backup.NeoApp
import com.machiav3lli.backup.data.entity.StorageFile
import com.machiav3lli.backup.data.preferences.pref_debugLog
import com.machiav3lli.backup.data.preferences.pref_debugLogToInternalStorage
import timber.log.Timber
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class DebugLogHandler {
    companion object {
        private const val DEBUG_LOG_FILENAME = "debug.log"
        private val logLock = Any()
        private val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")

        fun getDebugLogFile(): StorageFile? {
            return if (pref_debugLogToInternalStorage.value) {
                // Internal storage - for UI access (share/clear)
                NeoApp.context.filesDir?.let { filesDir ->
                    StorageFile(java.io.File(filesDir, DEBUG_LOG_FILENAME))
                }
            } else {
                // Backup root
                NeoApp.backupRoot?.let { backupRoot ->
                    backupRoot.findFile(DEBUG_LOG_FILENAME)
                        ?: backupRoot.createFile(DEBUG_LOG_FILENAME)
                }
            }
        }

        fun writeDebug(message: String) {
            synchronized(logLock) {
                try {
                    val timestamp = LocalDateTime.now().format(timestampFormatter)
                    val thread = Thread.currentThread()
                    val threadInfo = "Thread:${thread.name}/${thread.id}"
                    val line = "[$timestamp] [$threadInfo] $message\n"

                    // Use internal storage if: preference set OR backupRoot not initialized yet
                    if (pref_debugLogToInternalStorage.value || !NeoApp.isBackupRootInitialized) {
                        // Write to internal storage
                        NeoApp.context.filesDir?.let { filesDir ->
                            val logFile = java.io.File(filesDir, DEBUG_LOG_FILENAME)
                            java.io.FileWriter(logFile, true).use { writer ->
                                writer.write(line)
                            }
                        }
                    } else {
                        // Write to backup directory (backupRoot is initialized)
                        NeoApp.backupRoot?.file?.let { rootFile ->
                            val logFile = java.io.File(rootFile, DEBUG_LOG_FILENAME)
                            java.io.FileWriter(logFile, true).use { writer ->
                                writer.write(line)
                            }
                        } ?: run {
                            // Pure SAF fallback
                            val logFile = getDebugLogFile() ?: return
                            logFile.appendText(line)
                        }
                    }
                } catch (e: Exception) {
                    Timber.e("Failed to write debug log: $e")
                }
            }
        }

        private fun StorageFile.appendText(text: String) {
            try {
                appendOutputStream()?.use { out ->
                    out.write(text.toByteArray(StandardCharsets.UTF_8))
                }
            } catch (e: Exception) {
                Timber.e("Failed to append to debug log: $e")
            }
        }
    }
}

inline fun debugLog(lazyMessage: () -> String) {
    if (pref_debugLog.value) {
        DebugLogHandler.writeDebug(lazyMessage())
    }
}

fun getCompactStackTrace(): String = Thread.currentThread().stackTrace
    .drop(3).take(10).joinToString(" | ")
