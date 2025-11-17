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

import android.text.format.Formatter
import com.machiav3lli.backup.NeoApp
import com.machiav3lli.backup.data.entity.StorageFile
import com.machiav3lli.backup.utils.BACKUP_DATE_TIME_FORMATTER
import timber.log.Timber
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class ScheduleLogHandler {
    companion object {
        private const val SCHEDULE_LOG_FILENAME = "schedules.log"
        private val logLock = Any()

        fun getScheduleLogFile(): StorageFile? {
            return NeoApp.backupRoot?.let { backupRoot ->
                try {
                    backupRoot.findFile(SCHEDULE_LOG_FILENAME)
                        ?: backupRoot.createFile(SCHEDULE_LOG_FILENAME)
                } catch (e: Exception) {
                    Timber.e("Failed to get schedule log file: $e")
                    null
                }
            }
        }

        fun writeScheduleStart(scheduleName: String, timestamp: LocalDateTime) {
            val logFile = getScheduleLogFile() ?: return
            try {
                val dateStr = BACKUP_DATE_TIME_FORMATTER.format(timestamp)
                val header = "\n${"=".repeat(60)}\nSchedule: $scheduleName - $dateStr\n${"=".repeat(60)}\n"
                logFile.appendText(header)
            } catch (e: Exception) {
                Timber.e("Failed to write schedule start: $e")
            }
        }

        fun writeAppDecision(
            packageName: String,
            packageLabel: String,
            decision: String, // "BACKUP" or "SKIP"
            reason: String,
            sizeBytes: Long = 0,
            timestamp: LocalDateTime = LocalDateTime.now()
        ) {
            val logFile = getScheduleLogFile() ?: return
            try {
                val timeStr = timestamp.format(DateTimeFormatter.ofPattern("HH:mm:ss"))
                val sizeStr = if (decision == "BACKUP" && sizeBytes > 0) {
                    formatSize(sizeBytes)
                } else "-"
                val line = "[$timeStr] $packageName ($packageLabel) | $decision | $reason | $sizeStr\n"
                logFile.appendText(line)
            } catch (e: Exception) {
                Timber.e("Failed to write app decision: $e")
            }
        }

        fun writeScheduleEnd(backedUpCount: Int, skippedCount: Int, totalSizeBytes: Long, timestamp: LocalDateTime) {
            val logFile = getScheduleLogFile() ?: return
            try {
                val timeStr = timestamp.format(DateTimeFormatter.ofPattern("HH:mm:ss"))
                val totalSize = formatSize(totalSizeBytes)
                val summary = "[$timeStr] Completed: $backedUpCount backed up, $skippedCount skipped - Total: $totalSize\n"
                logFile.appendText(summary)
            } catch (e: Exception) {
                Timber.e("Failed to write schedule end: $e")
            }
        }

        private fun formatSize(bytes: Long): String {
            // Use Android's Formatter to match UI formatting (base 1000 / SI units)
            return Formatter.formatFileSize(NeoApp.context, bytes)
        }

        private fun StorageFile.appendText(text: String) {
            synchronized(logLock) {
                try {
                    appendOutputStream()?.use { out ->
                        out.write(text.toByteArray(StandardCharsets.UTF_8))
                    }
                } catch (e: Exception) {
                    Timber.e("Failed to append to schedule log: $e")
                }
            }
        }
    }
}

