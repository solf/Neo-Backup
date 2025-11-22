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
import com.topjohnwu.superuser.io.SuFile
import timber.log.Timber
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

object ApkDeduplicationHelper {
    // Per-package locks for APK operations to ensure atomicity
    private val packageLocks = ConcurrentHashMap<String, ReentrantLock>()

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
     * Generate dedup directory name from version code and APK sizes
     * Uses metadata (size, count) instead of hashing for speed
     * Format: {versionCode}_{count}_{totalSize}
     * 
     * This is reliable for APKs because:
     * - APKs are compressed ZIP archives
     * - Even tiny content changes alter compressed size
     * - Collision probability is astronomically low
     */
    fun getApkDedupDirName(versionCode: Int, apkPaths: Array<String>): String {
        val totalSize = apkPaths.sumOf { path ->
            try {
                val file = File(path)
                if (file.exists()) {
                    file.length()
                } else {
                    // Try with SuFile for root access
                    SuFile(path).length()
                }
            } catch (e: Exception) {
                Timber.w(e, "Failed to get size for: $path")
                0L
            }
        }
        val count = apkPaths.size
        return "${versionCode}_${count}_${totalSize}"
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
     * Format: apk/{versionCode}_{count}_{totalSize}
     */
    fun getRelativeApkPath(versionCode: Int, apkPaths: Array<String>): String {
        return "apk/${getApkDedupDirName(versionCode, apkPaths)}"
    }

    /**
     * Check if a path string refers to a deduplicated APK location
     */
    fun isDeduplicatedApkPath(path: String?): Boolean {
        return path != null && path.startsWith("apk/")
    }
}

