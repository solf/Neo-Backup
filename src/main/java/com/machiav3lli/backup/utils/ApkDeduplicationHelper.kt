/*
 * Neo Backup: open-source apps backup and restore app.
 * Copyright (C) 2020  Antonios Hazim
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
package com.machiav3lli.backup.utils

import com.machiav3lli.backup.data.dbs.entity.Backup
import com.machiav3lli.backup.data.entity.StorageFile
import com.machiav3lli.backup.manager.handler.debugLog
import com.topjohnwu.superuser.io.SuFile
import timber.log.Timber
import java.io.File
import java.io.FileInputStream
import java.security.MessageDigest
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

object ApkDeduplicationHelper {
    // Per-package locks for APK operations to ensure atomicity
    private val packageLocks = ConcurrentHashMap<String, ReentrantLock>()
    
    // Size to read for hash calculation (1 MB)
    private const val HASH_READ_SIZE = 1024 * 1024

    /**
     * Get or create lock for specific package
     */
    private fun getPackageLock(packageName: String): ReentrantLock {
        return packageLocks.getOrPut(packageName) { ReentrantLock() }
    }

    /**
     * Execute APK operation with per-package locking to prevent race conditions
     */
    fun <T> withPackageApkLock(packageName: String, block: () -> T): T {
        return getPackageLock(packageName).withLock {
            block()
        }
    }

    /**
     * Sanitize version name for use in filesystem paths
     * Replaces any character not alphanumeric, period, hyphen, or underscore with underscore
     * Trims to maximum 50 characters
     */
    private fun sanitizeVersionName(versionName: String?): String {
        if (versionName.isNullOrBlank()) return "unknown"
        // Replace anything not alphanumeric, period, hyphen, or underscore with underscore
        val sanitized = versionName.replace(Regex("[^a-zA-Z0-9._-]"), "_")
        // Trim to max 50 chars
        return sanitized.take(50)
    }

    /**
     * Calculate short hash suffix from all APK files
     * Hashes first 1MB of each APK part (or entire file if smaller) plus file size
     * Returns last 8 characters of SHA-256 hash
     */
    private fun calculateApkHashSuffix(apkPaths: Array<String>): String {
        try {
            val digest = MessageDigest.getInstance("SHA-256")
            
            // Hash each APK part in order
            for (apkPath in apkPaths) {
                val file = File(apkPath)
                val fileSize = if (file.exists()) {
                    file.length()
                } else {
                    // Try with SuFile for root access
                    val suFile = SuFile(apkPath)
                    if (!suFile.exists()) {
                        Timber.w("APK file does not exist: $apkPath")
                        continue
                    }
                    suFile.length()
                }
                
                // Include file size in hash to detect truncated files
                digest.update(fileSize.toString().toByteArray())

                // Read first 1MB or entire file, whichever is smaller
                val bytesToRead = minOf(HASH_READ_SIZE.toLong(), fileSize).toInt()
                val buffer = ByteArray(8192)
                var totalRead = 0

                FileInputStream(file).use { fis ->
                    while (totalRead < bytesToRead) {
                        val toRead = minOf(buffer.size, bytesToRead - totalRead)
                        val bytesRead = fis.read(buffer, 0, toRead)
                        if (bytesRead <= 0) break
                        digest.update(buffer, 0, bytesRead)
                        totalRead += bytesRead
                    }
                }
            }

            val hashBytes = digest.digest()
            val hashHex = hashBytes.joinToString("") { "%02x".format(it) }
            
            // Return last 8 characters for reasonable uniqueness
            return hashHex.takeLast(8)
        } catch (e: Exception) {
            Timber.e(e, "Failed to calculate APK hash for: ${apkPaths.joinToString()}")
            debugLog { "[ApkDedup] Hash calculation failed, using fallback for: ${apkPaths.joinToString { File(it).name }}" }
            // Return a fallback hash based on paths
            return apkPaths.joinToString("").hashCode().toString(16).takeLast(8).padStart(8, '0')
        }
    }

    /**
     * Generate dedup directory name from version name, version code, and APK hash
     * Hashes first 1MB of all APK parts for reliable uniqueness
     * Format: {sanitizedVersionName}_{versionCode}_{hashSuffix}
     */
    fun getApkDedupDirName(versionName: String?, versionCode: Int, apkPaths: Array<String>): String {
        val hashSuffix = calculateApkHashSuffix(apkPaths)
        return "${sanitizeVersionName(versionName)}_${versionCode}_${hashSuffix}"
    }

    /**
     * Check if APK directory contains matching APKs
     * Verifies that all expected APK files exist in the dedup directory
     * Returns true if all APKs match (exist with same names)
     */
    fun verifyApkMatch(dedupDir: StorageFile, apkPaths: Array<String>): Boolean {
        try {
            if (!dedupDir.exists() || !dedupDir.isDirectory) {
                return false
            }

            val dedupFiles = dedupDir.listFiles()
            val dedupFileNames = dedupFiles.mapNotNull { it.name }.toSet()

            // Check if all expected APKs exist in dedup directory
            for (apkPath in apkPaths) {
                val apkFileName = File(apkPath).name
                if (apkFileName !in dedupFileNames) {
                    Timber.d("APK not found in dedup dir: $apkFileName")
                    return false
                }
            }

            Timber.d("All APKs verified in dedup directory: ${dedupDir.path}")
            debugLog { "[ApkDedup] Verified APK match: ${dedupDir.name}" }
            return true
        } catch (e: Exception) {
            Timber.e(e, "Failed to verify APK match in: ${dedupDir.path}")
            return false
        }
    }

    /**
     * Count references to a specific apkStorageDir in the backups list
     * Used to determine if APK directory can be safely deleted
     */
    fun countReferences(backups: List<Backup>, apkStorageDir: String): Int {
        return backups.count { it.apkStorageDir == apkStorageDir }
    }

    /**
     * Parse apkStorageDir from a properties file
     * Returns the apkStorageDir value if present, null otherwise
     */
    fun parseApkStorageDirFromProperties(propsFile: StorageFile): String? {
        return try {
            val backup = Backup.createFrom(propsFile)
            backup?.apkStorageDir
        } catch (e: Exception) {
            Timber.e(e, "Failed to parse properties file: ${propsFile.path}")
            null
        }
    }

    /**
     * Get relative path to APK dedup directory within app backup directory
     * Format: apk/{sanitizedVersionName}_{versionCode}_{hashSuffix}
     */
    fun getRelativeApkPath(versionName: String?, versionCode: Int, apkPaths: Array<String>): String {
        return "apk/${getApkDedupDirName(versionName, versionCode, apkPaths)}"
    }

    /**
     * Check if a path string refers to a deduplicated APK location
     */
    fun isDeduplicatedApkPath(path: String?): Boolean {
        return path != null && path.startsWith("apk/")
    }
}

