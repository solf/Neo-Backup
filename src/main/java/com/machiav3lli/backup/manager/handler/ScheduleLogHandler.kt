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
import com.machiav3lli.backup.utils.BACKUP_DATE_TIME_FORMATTER
import timber.log.Timber
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class ScheduleLogHandler {
    companion object {
        private const val SCHEDULE_LOG_FILENAME = "schedules.log"

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
            return when {
                bytes < 1024 -> "${bytes}B"
                bytes < 1024 * 1024 -> String.format("%.1fKB", bytes / 1024.0)
                bytes < 1024 * 1024 * 1024 -> String.format("%.1fMB", bytes / (1024.0 * 1024))
                else -> String.format("%.2fGB", bytes / (1024.0 * 1024 * 1024))
            }
        }

        private fun StorageFile.appendText(text: String) {
            try {
                // Read existing content
                val existingContent = try {
                    readText()
                } catch (e: Exception) {
                    "" // File doesn't exist or can't be read
                }
                
                // Write combined content
                val newContent = existingContent + text
                outputStream()?.use { out ->
                    out.write(newContent.toByteArray(StandardCharsets.UTF_8))
                }
            } catch (e: Exception) {
                Timber.e("Failed to append to schedule log: $e")
            }
        }
    }
}

