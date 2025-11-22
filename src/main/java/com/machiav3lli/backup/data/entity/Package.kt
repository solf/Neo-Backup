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
package com.machiav3lli.backup.data.entity

import android.app.usage.StorageStats
import android.content.Context
import android.content.pm.PackageManager
import com.machiav3lli.backup.NeoApp
import com.machiav3lli.backup.data.dbs.entity.AppInfo
import com.machiav3lli.backup.data.dbs.entity.Backup
import com.machiav3lli.backup.data.dbs.entity.SpecialInfo
import com.machiav3lli.backup.data.preferences.traceBackups
import com.machiav3lli.backup.manager.handler.LogsHandler
import com.machiav3lli.backup.manager.handler.ShellCommands
import com.machiav3lli.backup.manager.handler.findBackups
import com.machiav3lli.backup.manager.handler.getPackageStorageStats
import com.machiav3lli.backup.ui.pages.pref_flatStructure
import com.machiav3lli.backup.ui.pages.pref_ignoreLockedInHousekeeping
import com.machiav3lli.backup.ui.pages.pref_paranoidBackupLists
import com.machiav3lli.backup.data.preferences.NeoPrefs
import com.machiav3lli.backup.manager.handler.debugLog
import com.machiav3lli.backup.utils.ChangeDetectionUtils
import com.machiav3lli.backup.utils.FileUtils
import com.machiav3lli.backup.utils.StorageLocationNotConfiguredException
import com.machiav3lli.backup.utils.SystemUtils
import com.machiav3lli.backup.utils.SystemUtils.getAndroidFolder
import com.machiav3lli.backup.utils.TraceUtils
import kotlinx.coroutines.runBlocking
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import timber.log.Timber
import java.io.File

// TODO consider separating package & backupsList to allow granular compose updates
data class Package private constructor(val packageName: String) : KoinComponent {
    private val neoPrefs: NeoPrefs by inject()
    lateinit var packageInfo: com.machiav3lli.backup.data.dbs.entity.PackageInfo
    var storageStats: StorageStats? = null
        private set

    val backupList: List<Backup>
        get() = NeoApp.getBackups(packageName)

    fun setBackupList(backups: List<Backup>) = NeoApp.putBackups(packageName, backups)

    // toPackageList
    internal constructor(
        context: Context,
        appInfo: AppInfo,
    ) : this(appInfo.packageName) {
        this.packageInfo = appInfo
        if (appInfo.installed) refreshStorageStats(context)
    }

    // special packages
    constructor(
        specialInfo: SpecialInfo,
    ) : this(specialInfo.packageName) {
        this.packageInfo = specialInfo
    }

    // schedule, getInstalledPackageList, packages from PackageManager
    constructor(
        context: Context,
        packageInfo: android.content.pm.PackageInfo,
    ) : this(packageInfo.packageName) {
        this.packageInfo = AppInfo(context, packageInfo)
        refreshStorageStats(context)
    }

    // updateDataOf, NOLABEL (= packageName not found)
    constructor(
        context: Context,
        packageName: String,
    ) : this(packageName) {
        try {
            val pi = context.packageManager.getPackageInfo(
                this.packageName,
                PackageManager.GET_PERMISSIONS
            )
            this.packageInfo = AppInfo(context, pi)
            refreshStorageStats(context)
        } catch (e: PackageManager.NameNotFoundException) {
            try {
                this.packageInfo = SpecialInfo.getSpecialInfos(context)
                    .find { it.packageName == this.packageName }!!
            } catch (e: Throwable) {
                //TODO hg42 Timber.i("$packageName is not installed")
                this.packageInfo = latestBackup?.toAppInfo() ?: run {
                    throw AssertionError(
                        "Backup History is empty and package is not installed. The package is completely unknown?",     //TODO hg42 remove package from database???
                        e
                    )
                }
            }
        }
    }

    fun runOrLog(todo: () -> Unit) {
        try {
            todo()
        } catch (e: Throwable) {
            LogsHandler.unexpectedException(e, packageName)
        }
    }

    private fun isPlausiblePath(path: String?): Boolean {
        return !path.isNullOrEmpty() &&
                path.contains(packageName) &&
                path != NeoApp.backupRoot?.path
    }

    fun refreshStorageStats(context: Context): Boolean {
        return try {
            storageStats = context.getPackageStorageStats(packageName)
            true
        } catch (e: PackageManager.NameNotFoundException) {
            LogsHandler.logException(e, "Could not refresh StorageStats. Package was not found")
            false
        }
    }

    fun refreshFromPackageManager(context: Context): Boolean {
        Timber.d("Trying to refresh package information for $packageName from PackageManager")
        try {
            val pi =
                context.packageManager.getPackageInfo(packageName, PackageManager.GET_PERMISSIONS)
            packageInfo = AppInfo(context, pi)
            refreshStorageStats(context)
        } catch (e: PackageManager.NameNotFoundException) {
            LogsHandler.logException(e, "$packageName is not installed. Refresh failed")
            return false
        }
        return true
    }

    fun getBackupsFromBackupDir(): List<Backup> {
        // TODO hg42 may also find glob *packageName* for now so we need to take the correct package
        return NeoApp.context.findBackups(packageName)[packageName] ?: emptyList()
    }

    fun refreshBackupList() {
        traceBackups { "<$packageName> refreshbackupList" }
        val backups = getBackupsFromBackupDir()
        setBackupList(backups)
    }

    @Throws(
        FileUtils.BackupLocationInAccessibleException::class,
        StorageLocationNotConfiguredException::class
    )
    fun getAppBackupBaseDir(
        packageName: String = this.packageName,
        create: Boolean = false,
    ): StorageFile? {
        return try {
            if (pref_flatStructure.value) {
                NeoApp.backupRoot
            } else {
                when {
                    create -> {
                        NeoApp.backupRoot?.ensureDirectory(packageName)
                    }

                    else   -> {
                        NeoApp.backupRoot?.findFile(packageName)
                    }
                }
            }
        } catch (e: Throwable) {
            LogsHandler.unexpectedException(e)
            null
        }
    }

    private fun addBackupToList(vararg backup: Backup) {
        traceBackups {
            "<$packageName> addBackupToList: ${
                TraceUtils.formatBackups(backup.toList())
            } into ${
                TraceUtils.formatBackups(backupsNewestFirst)
            } ${TraceUtils.methodName(2)}"
        }
        setBackupList(backupList + backup)
    }

    private fun replaceBackupFromList(backup: Backup, newBackup: Backup) {
        traceBackups {
            "<$packageName> replaceBackupFromList: ${
                TraceUtils.formatBackups(listOf(newBackup))
            } in ${
                TraceUtils.formatBackups(backupsNewestFirst)
            } ${TraceUtils.methodName(2)}"
        }
        setBackupList(backupList.filterNot {
            it.packageName == backup.packageName
                    && it.backupDate == backup.backupDate
        }.plus(newBackup))
    }

    private fun removeBackupFromList(backup: Backup) {
        traceBackups {
            "<$packageName> removeBackupFromList: ${
                TraceUtils.formatBackups(listOf(backup))
            } from ${
                TraceUtils.formatBackups(backupsNewestFirst)
            } ${TraceUtils.methodName(2)}"
        }
        setBackupList(backupList.filterNot {
            it.packageName == backup.packageName
                    && it.backupDate == backup.backupDate
        })
    }

    fun addNewBackup(backup: Backup) {
        traceBackups { "<${backup.packageName}> add backup ${backup.backupDate}" }
        if (pref_paranoidBackupLists.value)
            refreshBackupList()  // no more necessary, because members file/dir/tag are set by createBackup
        else {
            addBackupToList(backup)
        }
    }

    private fun _deleteBackup(backup: Backup) {
        traceBackups { "<${backup.packageName}> delete backup ${backup.backupDate}" }
        if (backup.packageName != packageName) {    //TODO hg42 probably paranoid
            throw RuntimeException("Asked to delete a backup of ${backup.packageName} but this object is for $packageName")
        }
        val parent = backup.file?.parent
        runOrLog { backup.file?.delete() }    // first, it could be inside dir
        runOrLog { backup.dir?.deleteRecursive() }
        parent?.let {
            if (isPlausiblePath(parent.path))
                runCatching { it.delete() }   // delete the directory (but never the contents)
        }
        if (!pref_paranoidBackupLists.value)
            runOrLog {
                removeBackupFromList(backup)
            }
    }

    fun deleteBackup(backup: Backup) {
        _deleteBackup(backup)
        if (pref_paranoidBackupLists.value)
            runOrLog { refreshBackupList() }                // get real state of file system
    }

    fun rewriteBackup(
        backup: Backup,
        changedBackup: Backup,
    ) {      //TODO hg42 change to rewriteBackup(backup: Backup, applyParameters)
        traceBackups { "<${changedBackup.packageName}> rewrite backup ${changedBackup.backupDate}" }
        if (changedBackup.dir == null) changedBackup.dir = backup.dir
        if (changedBackup.file == null) changedBackup.file = backup.file
        if (changedBackup.packageName != packageName) {             //TODO hg42 probably paranoid
            throw RuntimeException("Asked to rewrite a backup of ${changedBackup.packageName} but this object is for $packageName")
        }
        if (changedBackup.backupDate != backup.backupDate) {        //TODO hg42 probably paranoid
            throw RuntimeException("Asked to rewrite a backup from ${changedBackup.backupDate} but the original backup is from ${backup.backupDate}")
        }
        runOrLog {
            synchronized(this) {
                backup.file?.apply {
                    writeText(changedBackup.toSerialized())
                }
            }
        }
        if (pref_paranoidBackupLists.value)
            runOrLog { refreshBackupList() }                // get real state of file system
        else {
            replaceBackupFromList(backup, changedBackup)
        }
    }

    fun deleteAllBackups() {
        val backups = backupsNewestFirst.toMutableList()
        while (backups.isNotEmpty()) {
            backups.removeLastOrNull()?.let { backup ->
                _deleteBackup(backup)
            }
        }
        if (pref_paranoidBackupLists.value)
            runOrLog { refreshBackupList() }                // get real state of file system only once
    }

    fun deleteOldestBackups(keep: Int) {
        // the algorithm could eventually be more elegant, without managing two lists,
        // but it's on the safe side for now
        val backups = backupsNewestFirst.toMutableList()
        val deletableBackups = backups.let {
            if (pref_ignoreLockedInHousekeeping.value) it
            else it.filterNot { it.persistent }
        }.drop(keep).toMutableList()
        traceBackups {
            "<$packageName> deleteOldestBackups keep=$keep ${
                TraceUtils.formatBackups(
                    backups
                )
            } --> delete ${TraceUtils.formatBackups(deletableBackups)}"
        }
        while (deletableBackups.isNotEmpty()) {
            deletableBackups.removeLastOrNull()?.let { backup ->
                _deleteBackup(backup)
            }
        }
    }

    val backupsNewestFirst: List<Backup>
        get() = backupList.sortedByDescending { it.backupDate }

    val latestBackup: Backup?
        get() = backupList.maxByOrNull { it.backupDate }

    val numberOfBackups: Int get() = backupList.size

    val isApp: Boolean
        get() = packageInfo is AppInfo && !packageInfo.isSpecial

    val isInstalled: Boolean
        get() = (isApp && (packageInfo as AppInfo).installed) || packageInfo.isSpecial

    val isDisabled: Boolean
        get() = isInstalled && !isSpecial && !(packageInfo is AppInfo && (packageInfo as AppInfo).enabled)

    val isSystem: Boolean
        get() = packageInfo.isSystem || packageInfo.isSpecial

    val isSpecial: Boolean
        get() = packageInfo.isSpecial

    val packageLabel: String
        get() = packageInfo.packageLabel.ifEmpty { packageName }

    val versionCode: Int
        get() = packageInfo.versionCode

    val versionName: String?
        get() = packageInfo.versionName

    val hasBackups: Boolean
        get() = backupList.isNotEmpty()

    val apkPath: String
        get() = if (isApp) (packageInfo as AppInfo).apkDir ?: "" else ""

    val dataPath: String
        get() = if (isApp) (packageInfo as AppInfo).dataDir ?: "" else ""

    val devicesProtectedDataPath: String
        get() = if (isApp) (packageInfo as AppInfo).deDataDir ?: "" else ""

    val iconData: Any
        get() = if (isSpecial) packageInfo.icon
        else "android.resource://${packageName}/${packageInfo.icon}"

    fun getExternalDataPath(): String {
        val user = ShellCommands.currentProfile.toString()
        return getAndroidFolder("data", user, SystemUtils::isWritablePath)
            ?.absolutePath
            ?.plus("${File.separator}$packageName")
            ?: ""
    }

    fun getObbFilesPath(): String {
        val user = ShellCommands.currentProfile.toString()
        return getAndroidFolder("obb", user, SystemUtils::isWritablePath)
            ?.absolutePath
            ?.plus("${File.separator}$packageName")
            ?: ""
    }

    fun getMediaFilesPath(): String {
        val user = ShellCommands.currentProfile.toString()
        return getAndroidFolder("media", user, SystemUtils::isWritablePath)
            ?.absolutePath
            ?.plus("${File.separator}$packageName")
            ?: ""
    }

    /**
     * Returns the list of additional apks (excluding the main apk), if the app is installed
     *
     * @return array of absolute filepaths pointing to one or more split apks or empty if
     * the app is not splitted
     */
    val apkSplits: Array<String>
        get() = packageInfo.splitSourceDirs

    val isUpdated: Boolean
        get() = latestBackup?.let { it.versionCode < versionCode } ?: false

    val isNew: Boolean
        get() = !hasBackups && !isSystem    //TODO hg42 && versionCode > lastSeenVersionCode

    val isNewOrUpdated: Boolean
        get() = isUpdated || isNew

    val hasDataChangedSinceLastBackup: Boolean
        get() {
            val lastBackup = latestBackup ?: return true // No backup = treat as changed
            
            // Note: backupDate is stored as LocalDateTime (timezone-agnostic) and interpreted
            // in the current system timezone. This could cause minor inaccuracies if the device
            // timezone changes between backup creation and this check (e.g., travel, DST change).
            // However, with typical daily backup frequency (~24h), timezone shifts (max ~12h)
            // are unlikely to cause false negatives. Worst case: unnecessary re-backup.
            val lastBackupTimeMillis = lastBackup.backupDate.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli()
            
            // Check version code first (quick check - catches both upgrades and downgrades)
            if (lastBackup.versionCode != versionCode) {
                debugLog { "[ChangeDetect] $packageName: version changed (${lastBackup.versionCode} -> $versionCode)" }
                Timber.d("[ChangeDetect] $packageName: version changed (${lastBackup.versionCode} -> $versionCode)")
                return true
            }
            
            // Get preferences for deep scanning
            val scanDepth = try { neoPrefs.changeDetectionScanDepth.value } catch (e: Exception) { 3 }
            
            // Check data directories if they exist
            try {
                val dirsToCheck = mutableListOf<Pair<String, String>>() // path, type
                
                val dataDirPath = dataPath
                if (dataDirPath.isNotEmpty()) {
                    dirsToCheck.add(Pair(dataDirPath, "data"))
                }
                
                val deDirPath = devicesProtectedDataPath
                if (deDirPath.isNotEmpty()) {
                    dirsToCheck.add(Pair(deDirPath, "dedata"))
                }
                
                // Check external data if backup included it
                if (lastBackup.hasExternalData) {
                    val extPath = getExternalDataPath()
                    if (extPath.isNotEmpty()) {
                        dirsToCheck.add(Pair(extPath, "external"))
                    }
                }
                
                // Check obb data if backup included it
                if (lastBackup.hasObbData) {
                    val obbPath = getObbFilesPath()
                    if (obbPath.isNotEmpty()) {
                        dirsToCheck.add(Pair(obbPath, "obb"))
                    }
                }
                
                // Check media data if backup included it
                if (lastBackup.hasMediaData) {
                    val mediaPath = getMediaFilesPath()
                    if (mediaPath.isNotEmpty()) {
                        dirsToCheck.add(Pair(mediaPath, "media"))
                    }
                }
                
                // Get last changed type for prioritization
                val lastChangedType = NeoApp.getHotPath("$packageName:lastChangedType")
                
                // PHASE 1: Quick check - scan all hot-paths (starting with last-changed type)
                // Reorder to check last-changed type first
                val prioritizedDirs = if (lastChangedType != null) {
                    val lastChangedEntry = dirsToCheck.find { it.second == lastChangedType }
                    if (lastChangedEntry != null) {
                        listOf(lastChangedEntry) + dirsToCheck.filter { it.second != lastChangedType }
                    } else {
                        dirsToCheck
                    }
                } else {
                    dirsToCheck
                }
                
                for ((dirPath, dirType) in prioritizedDirs) {
                    val dir = RootFile(dirPath)
                    if (!dir.exists()) continue
                    
                    val hotPath = NeoApp.getHotPath("$packageName:$dirType")
                    if (hotPath != null) {
                        // Phase 1: Check only the hot-path (depth 0, just the specific path)
                        val hotFile = RootFile(dir, hotPath)
                        if (hotFile.exists()) {
                            val hotTimestamp = hotFile.lastModified()
                            if (hotTimestamp > lastBackupTimeMillis) {
                                debugLog { "[ChangeDetect] $packageName: changes detected in $dirType at $hotPath (timestamp=$hotTimestamp >= $lastBackupTimeMillis) (hot-path ✓ PHASE-1)" }
                                Timber.d("[ChangeDetect] $packageName: changes detected in $dirType at $hotPath (timestamp=$hotTimestamp >= $lastBackupTimeMillis) (hot-path ✓ PHASE-1)")
                                
                                // Update last changed type
                                NeoApp.updateHotPath("$packageName:lastChangedType", dirType)
                                
                                return true
                            }
                        }
                    }
                }
                
                debugLog { "[ChangeDetect] $packageName: PHASE-1 complete (hot-paths), no changes found, proceeding to PHASE-2 (full scan)" }
                Timber.d("[ChangeDetect] $packageName: PHASE-1 complete (hot-paths), no changes found, proceeding to PHASE-2 (full scan)")
                
                // PHASE 2: Full BFS scan (only if Phase 1 found nothing)
                for ((dirPath, dirType) in prioritizedDirs) {
                    val dir = RootFile(dirPath)
                    if (!dir.exists()) continue
                    
                    val hotPath = NeoApp.getHotPath("$packageName:$dirType")
                    val result = ChangeDetectionUtils.scanForChanges(
                        rootDir = dir,
                        thresholdTimestamp = lastBackupTimeMillis,
                        maxDepth = scanDepth,
                        hotPath = hotPath
                    )
                    
                    if (result.hasChanges) {
                        debugLog { "[ChangeDetect] $packageName: changes detected in $dirType at ${result.foundPath} (timestamp=${result.foundTimestamp} >= $lastBackupTimeMillis) (PHASE-2 full-scan)" }
                        Timber.d("[ChangeDetect] $packageName: changes detected in $dirType at ${result.foundPath} (timestamp=${result.foundTimestamp} >= $lastBackupTimeMillis) (PHASE-2 full-scan)")
                        
                        // Update hot path and last changed type
                        result.foundPath?.let { foundPath ->
                            NeoApp.updateHotPath("$packageName:$dirType", foundPath)
                            NeoApp.updateHotPath("$packageName:lastChangedType", dirType)
                        }
                        
                        return true
                    }
                }
                
                debugLog { "[ChangeDetect] $packageName: no changes since ${lastBackup.backupDate} (scanned depth=$scanDepth, 2-phase complete)" }
                Timber.d("[ChangeDetect] $packageName: no changes since ${lastBackup.backupDate} (scanned depth=$scanDepth, 2-phase complete)")
                return false
                
            } catch (e: Exception) {
                // If we can't check, treat as changed to be safe
                debugLog { "[ChangeDetect] $packageName: error checking data, treating as changed: ${e.message}" }
                Timber.d("[ChangeDetect] $packageName: error checking data, treating as changed: ${e.message}")
                return true
            }
        }

    val isModifiedOrNew: Boolean
        get() = hasDataChangedSinceLastBackup || isNew

    val hasApk: Boolean
        get() = backupList.any { it.hasApk }

    val hasData: Boolean
        get() = backupList.any {
            it.hasAppData || it.hasExternalData || it.hasDevicesProtectedData ||
                    it.hasObbData || it.hasMediaData
        }

    val hasAppData: Boolean
        get() = backupList.any { it.hasAppData }

    val hasExternalData: Boolean
        get() = backupList.any { it.hasExternalData }

    val hasDevicesProtectedData: Boolean
        get() = backupList.any { it.hasDevicesProtectedData }

    val hasObbData: Boolean
        get() = backupList.any { it.hasObbData }

    val hasMediaData: Boolean
        get() = backupList.any { it.hasMediaData }

    val appBytes: Long
        get() = if (packageInfo.isSpecial) 0 else storageStats?.appBytes ?: 0

    val dataBytes: Long
        get() = if (packageInfo.isSpecial) 0 else storageStats?.dataBytes ?: 0

    val backupBytes: Long
        get() = latestBackup?.size ?: 0

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other == null || javaClass != other.javaClass) return false
        val pkg = other as Package
        return packageName == pkg.packageName
                && this.packageInfo == pkg.packageInfo
                && storageStats == pkg.storageStats
                && backupList == pkg.backupList
    }

    override fun hashCode(): Int {
        var hash = 7
        hash = 31 * hash + packageName.hashCode()
        hash = 31 * hash + packageInfo.hashCode()
        hash = 31 * hash + storageStats.hashCode()
        hash = 31 * hash + backupList.hashCode()
        return hash
    }

    override fun toString(): String {
        return "Package{" +
                "packageName=" + packageName +
                ", appInfo=" + packageInfo +
                ", storageStats=" + storageStats +
                ", backupList=" + backupList +
                '}'
    }

    companion object {

        fun invalidateCacheForPackage(packageName: String = "") {
            if (packageName.isEmpty())
                StorageFile.invalidateCache()
            else
                StorageFile.invalidateCache {
                    it.contains(packageName)            // also matches *packageName* !
                }
        }

        fun invalidateBackupCacheForPackage(packageName: String = "") {
            if (packageName.isEmpty())
                StorageFile.invalidateCache {
                    true //it.startsWith(backupDirConfigured)
                }
            else
                StorageFile.invalidateCache {
                    //it.startsWith(backupDirConfigured) &&
                    it.contains(packageName)
                }
        }

        fun invalidateSystemCacheForPackage(packageName: String = "") {
            if (packageName.isEmpty())
                StorageFile.invalidateCache {
                    true //!it.startsWith(backupDirConfigured)
                }
            else
                StorageFile.invalidateCache {
                    //!it.startsWith(backupDirConfigured) &&
                    it.contains(packageName)
                }
        }
    }
}
