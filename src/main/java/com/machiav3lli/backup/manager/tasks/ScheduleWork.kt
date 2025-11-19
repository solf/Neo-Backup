package com.machiav3lli.backup.manager.tasks

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.os.Build
import androidx.compose.ui.util.fastForEach
import androidx.core.app.NotificationCompat
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingPeriodicWorkPolicy
import androidx.work.ExistingWorkPolicy
import androidx.work.ForegroundInfo
import androidx.work.OneTimeWorkRequest
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.OutOfQuotaPolicy
import androidx.work.PeriodicWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import androidx.work.workDataOf
import com.machiav3lli.backup.ACTION_CANCEL_SCHEDULE
import com.machiav3lli.backup.EXTRA_NAME
import com.machiav3lli.backup.EXTRA_PERIODIC
import com.machiav3lli.backup.EXTRA_SCHEDULE_ID
import com.machiav3lli.backup.MODE_UNSET
import com.machiav3lli.backup.NeoApp
import com.machiav3lli.backup.USE_CENTRALIZED_FOREGROUND_INSTEAD_OF_LEGACY
import com.machiav3lli.backup.R
import com.machiav3lli.backup.data.dbs.entity.Schedule
import com.machiav3lli.backup.data.dbs.repository.AppExtrasRepository
import com.machiav3lli.backup.data.dbs.repository.BlocklistRepository
import com.machiav3lli.backup.data.dbs.repository.PackageRepository
import com.machiav3lli.backup.data.dbs.repository.ScheduleRepository
import com.machiav3lli.backup.data.preferences.pref_autoLogAfterSchedule
import com.machiav3lli.backup.data.preferences.pref_autoLogSuspicious
import com.machiav3lli.backup.data.preferences.traceSchedule
import com.machiav3lli.backup.manager.handler.debugLog
import com.machiav3lli.backup.manager.handler.getDebugStackTrace
import com.machiav3lli.backup.manager.handler.LogsHandler
import com.machiav3lli.backup.manager.handler.ScheduleLogHandler
import com.machiav3lli.backup.manager.handler.WorkHandler
import com.machiav3lli.backup.manager.handler.getInstalledPackageList
import com.machiav3lli.backup.manager.handler.showNotification
import com.machiav3lli.backup.manager.services.CommandReceiver
import com.machiav3lli.backup.ui.activities.NeoActivity
import com.machiav3lli.backup.ui.pages.pref_fakeScheduleDups
import com.machiav3lli.backup.ui.pages.pref_useForegroundInJob
import com.machiav3lli.backup.ui.pages.pref_useForegroundInService
import com.machiav3lli.backup.ui.pages.supportInfo
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import com.machiav3lli.backup.ui.pages.textLog
import com.machiav3lli.backup.utils.FileUtils
import com.machiav3lli.backup.utils.StorageLocationNotConfiguredException
import com.machiav3lli.backup.utils.SystemUtils
import com.machiav3lli.backup.utils.calcRuntimeDiff
import com.machiav3lli.backup.utils.extensions.Android
import com.machiav3lli.backup.utils.extensions.takeUntilSignal
import com.machiav3lli.backup.utils.filterPackages
import kotlinx.collections.immutable.toPersistentSet
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.withContext
import org.koin.core.component.KoinComponent
import org.koin.core.component.inject
import org.koin.java.KoinJavaComponent.get
import timber.log.Timber
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

data class ScheduleStats(
    val backedUpCount: Int = 0,
    val skippedCount: Int = 0,
    val totalSize: Long = 0L
)

class ScheduleWork(
    private val context: Context,
    params: WorkerParameters
) : CoroutineWorker(context, params), KoinComponent {
    private val packageRepo: PackageRepository by inject()
    private val scheduleRepo: ScheduleRepository by inject()
    private val blocklistRepo: BlocklistRepository by inject()
    private val appExtrasRepo: AppExtrasRepository by inject()

    private var scheduleId = inputData.getLong(EXTRA_SCHEDULE_ID, -1L)
    private val notificationId = SystemUtils.now.toInt()
    private val fetchingNotificationId = SystemUtils.now.toInt()
    private var notification: Notification? = null
    private val scheduleJob = Job()

    override suspend fun getForegroundInfo(): ForegroundInfo {
        debugLog { "[NOTIF-FOREGROUND] ScheduleWork.getForegroundInfo() ENTRY: scheduleId=$scheduleId, notificationId=$notificationId\n${getDebugStackTrace()}" }
        val notification = createForegroundNotification()
        val title = notification.extras?.getCharSequence("android.title")?.toString() ?: ""
        val foregroundInfo = ForegroundInfo(
            notification.hashCode(),
            notification,
            if (Android.minSDK(Build.VERSION_CODES.Q)) ServiceInfo.FOREGROUND_SERVICE_TYPE_DATA_SYNC
            else 0
        )
        debugLog { "[NOTIF-FOREGROUND] ScheduleWork.getForegroundInfo() returning ForegroundInfo: id=${notification.hashCode()}, scheduleId=$scheduleId, title='$title'" }
        return foregroundInfo
    }

    override suspend fun doWork(): Result = withContext(Dispatchers.IO + scheduleJob) {
        try {
            scheduleId = inputData.getLong(EXTRA_SCHEDULE_ID, -1L)
            val name = inputData.getString(EXTRA_NAME) ?: ""

            debugLog { "ScheduleWork.doWork() ENTRY: scheduleId=$scheduleId, name='$name'" }

            if (USE_CENTRALIZED_FOREGROUND_INSTEAD_OF_LEGACY && pref_useForegroundInJob.value) {
                debugLog { "[NOTIF-FOREGROUND] ScheduleWork.doWork() setting foreground for entire schedule: scheduleId=$scheduleId" }
                setForeground(getForegroundInfo())
            }

            NeoApp.wakelock(true)

            traceSchedule {
                buildString {
                    append("[$scheduleId] %%%%% ScheduleWork starting for name='$name'")
                }
            }

            if (scheduleId < 0) {
                debugLog { "ScheduleWork.doWork() FAILED: scheduleId < 0" }
                return@withContext Result.failure()
            }

            // Atomically check if already running and register if not
            val putResult = runningSchedules.putIfAbsent(scheduleId, false)
            debugLog { "ScheduleWork.doWork() duplicate check: putIfAbsent result=$putResult" }
            if (putResult != null) {
                val message =
                    "[$scheduleId] duplicate schedule detected: $name (as designed, ignored)"
                debugLog { "ScheduleWork.doWork() DUPLICATE DETECTED: returning failure" }
                Timber.w(message)
                if (pref_autoLogSuspicious.value) {
                    textLog(
                        listOf(
                            message,
                            "--- autoLogSuspicious $scheduleId $name"
                        ) + supportInfo()
                    )
                }
                return@withContext Result.failure()
            }

            var totalBackedUp = 0
            var totalSkipped = 0
            var totalSize = 0L

            val repeatCount = 1 + pref_fakeScheduleDups.value
            debugLog { "ScheduleWork.doWork() repeat loop: count=$repeatCount (pref_fakeScheduleDups=${pref_fakeScheduleDups.value})" }
            repeat(repeatCount) { iteration ->
                debugLog { "ScheduleWork.doWork() repeat iteration $iteration/$repeatCount START" }
                val now = SystemUtils.now
                runningSchedules[scheduleId] = true
                val startTime = System.currentTimeMillis()
                val stats = processSchedule(name, now)
                val elapsed = System.currentTimeMillis() - startTime
                debugLog { "ScheduleWork.doWork() processSchedule() returned: stats=$stats, elapsed=${elapsed}ms" }
                if (stats == null) {
                    debugLog { "ScheduleWork.doWork() processSchedule() returned NULL, returning failure" }
                    return@withContext Result.failure()
                }
                // Accumulate statistics across multiple runs
                totalBackedUp += stats.backedUpCount
                totalSkipped += stats.skippedCount
                totalSize += stats.totalSize
                debugLog { "ScheduleWork.doWork() repeat iteration $iteration/$repeatCount END: accumulated backedUp=$totalBackedUp, skipped=$totalSkipped, size=$totalSize" }
            }

            debugLog { "ScheduleWork.doWork() all iterations complete, returning SUCCESS" }
            NeoApp.wakelock(false)

            Result.success()
        } catch (e: Exception) {
            debugLog { "ScheduleWork.doWork() EXCEPTION: ${e.javaClass.simpleName}: ${e.message}" }
            Timber.e(e)
            Result.failure()
        } finally {
            debugLog { "ScheduleWork.doWork() FINALLY: cleaning up, removing scheduleId=$scheduleId from runningSchedules" }
            runningSchedules.remove(scheduleId)
            NeoApp.wakelock(false)
        }
    }

    private suspend fun processSchedule(name: String, now: Long): ScheduleStats? =
        coroutineScope {
            debugLog { "processSchedule() ENTRY: name='$name', now=$now, scheduleId=$scheduleId" }
            
            // Show "Fetching apps list" notification while building package list
            showNotification(
                context,
                NeoActivity::class.java,
                fetchingNotificationId,
                String.format(
                    context.getString(R.string.fetching_action_list),
                    context.getString(R.string.backup)
                ),
                "",
                true
            )
            debugLog { "[NOTIF-CREATE] processSchedule() posted 'Fetching...' notification: id=$fetchingNotificationId" }
            
            val finishSignal = MutableStateFlow(false)
            val schedule = scheduleRepo.getSchedule(scheduleId)
            debugLog { "processSchedule() getSchedule: schedule=${if (schedule != null) "found" else "NULL"}" }
            if (schedule == null) {
                debugLog { "processSchedule() EXIT: schedule is NULL, returning null" }
                return@coroutineScope null
            }

            // Log schedule start
            ScheduleLogHandler.writeScheduleStart(name, java.time.LocalDateTime.now())

            val selectedItems = getFilteredPackages(schedule)
            debugLog { "processSchedule() getFilteredPackages: count=${selectedItems.size}" }

            if (selectedItems.isEmpty()) {
                debugLog { "processSchedule() EXIT: selectedItems is empty, returning null" }
                handleEmptySelectedItems(name)
                ScheduleLogHandler.writeScheduleEnd(name, 0, 0, 0, java.time.LocalDateTime.now())
                return@coroutineScope null
            }

            val worksList = mutableListOf<OneTimeWorkRequest>()
            val notificationManager =
                context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
            debugLog { "[NOTIF-CANCEL] ScheduleWork.processSchedule() canceling fetching notification: id=$fetchingNotificationId, scheduleId=$scheduleId\n${getDebugStackTrace()}" }
            notificationManager.cancel(fetchingNotificationId)
            debugLog { "[NOTIF-CANCEL] ScheduleWork.processSchedule() fetching notification canceled: id=$fetchingNotificationId" }

            val batchName = WorkHandler.getBatchName(name, now)
            get<WorkHandler>(WorkHandler::class.java).beginBatch(batchName)

            var errors = ""
            var resultsSuccess = true
            val finished = AtomicInteger(0)
            val queued = selectedItems.size
            val totalBackupSize = AtomicLong(0L)
            val backedUpCount = AtomicInteger(0)
            val skippedCount = AtomicInteger(0)

            val workJobs = selectedItems.map { packageName ->
                val oneTimeWorkRequest = AppActionWork.Request(
                    packageName = packageName,
                    mode = schedule.mode ?: MODE_UNSET,
                    backupBoolean = true,
                    notificationId = notificationId,
                    batchName = batchName,
                    immediate = false,
                    backupModifiedOnly = schedule.backupModifiedOnly
                )
                worksList.add(oneTimeWorkRequest)

                if (inputData.getBoolean(EXTRA_PERIODIC, false) && schedule != null)
                    scheduleRepo.update(schedule.copy(timePlaced = now))

                async {
                    get<WorkManager>(WorkManager::class.java).getWorkInfoByIdFlow(oneTimeWorkRequest.id)
                        .takeUntilSignal(finishSignal)
                        .collectLatest { workInfo ->
                            when (workInfo?.state) {
                                androidx.work.WorkInfo.State.SUCCEEDED,
                                androidx.work.WorkInfo.State.FAILED,
                                androidx.work.WorkInfo.State.CANCELLED -> {
                                    finished.incrementAndGet()
                                    val succeeded =
                                        workInfo.outputData.getBoolean("succeeded", false)
                                    val packageLabel =
                                        workInfo.outputData.getString("packageLabel") ?: ""
                                    val packageName = 
                                        workInfo.outputData.getString("packageName") ?: ""
                                    val error = workInfo.outputData.getString("error") ?: ""
                                    val backupSize = workInfo.outputData.getLong("backupSize", 0L)
                                    debugLog { "processSchedule() Flow: job terminal state=${workInfo.state}, pkg=$packageName, succeeded=$succeeded, finished=${finished.get()}/$queued" }
                                    
                                    // Track backed up vs skipped and log to schedule log
                                    run {
                                        val decision: String
                                        val reason: String
                                        val logSize: Long
                                        
                                        if (succeeded) {
                                            if (error.contains("Skipped")) {
                                                skippedCount.incrementAndGet()
                                                decision = "SKIP"
                                                reason = "no_changes"
                                                logSize = 0L
                                            } else {
                                                totalBackupSize.addAndGet(backupSize)
                                                backedUpCount.incrementAndGet()
                                                decision = "BACKUP"
                                                reason = if (schedule.backupModifiedOnly) "modified_data" else "scheduled_backup"
                                                logSize = backupSize
                                            }
                                        } else {
                                            decision = "FAILED"
                                            reason = error.ifEmpty { "unknown_error" }
                                            logSize = 0L
                                        }
                                        
                                        ScheduleLogHandler.writeAppDecision(
                                            name,
                                            packageName,
                                            packageLabel,
                                            decision,
                                            reason,
                                            sizeBytes = logSize
                                        )
                                    }
                                    debugLog { "processSchedule() Flow: counters after $packageName: backedUp=${backedUpCount.get()}, skipped=${skippedCount.get()}, finished=${finished.get()}/$queued" }

                                    if (error.isNotEmpty()) {
                                        errors = "$errors$packageLabel: ${
                                            LogsHandler.handleErrorMessages(
                                                context,
                                                error
                                            )
                                        }\n"
                                    }
                                    resultsSuccess = resultsSuccess && succeeded

                                    if (finished.get() >= queued) {
                                        debugLog { "processSchedule() Flow: finished >= queued (${finished.get()} >= $queued), setting finishSignal" }
                                        finishSignal.update { true }
                                        endSchedule(name, "all jobs finished")
                                        selectedItems.fastForEach {
                                            packageRepo.updatePackage(it)
                                        }
                                        // Log completion with actual statistics
                                        debugLog { "processSchedule() calling writeScheduleEnd: backedUp=${backedUpCount.get()}, skipped=${skippedCount.get()}, size=${totalBackupSize.get()}" }
                                        ScheduleLogHandler.writeScheduleEnd(
                                            scheduleName = name,
                                            backedUpCount = backedUpCount.get(),
                                            skippedCount = skippedCount.get(),
                                            totalSizeBytes = totalBackupSize.get(),
                                            timestamp = java.time.LocalDateTime.now()
                                        )
                                        debugLog { "processSchedule() writeScheduleEnd COMPLETED" }
                                    }
                                }

                                else                                   -> {
                                    if (finished.get() >= queued) {
                                        finishSignal.update { true }
                                        endSchedule(name, "all jobs finished")
                                        selectedItems.fastForEach {
                                            packageRepo.updatePackage(it)
                                        }
                                    }
                                }
                            }
                        }
                }
            }

            debugLog { "processSchedule() worksList.size=${worksList.size}, workJobs.size=${workJobs.size}" }
            if (worksList.isNotEmpty()) {
                if (beginSchedule(name, "queueing work")) {
                    debugLog { "processSchedule() calling WorkManager.beginWith().enqueue() with ${worksList.size} work items" }
                    get<WorkManager>(WorkManager::class.java)
                        .beginWith(worksList)
                        .enqueue()
                    debugLog { "processSchedule() enqueue COMPLETED, now calling workJobs.awaitAll() for ${workJobs.size} async jobs" }
                    val awaitStartTime = System.currentTimeMillis()
                    workJobs.awaitAll()
                    val awaitElapsed = System.currentTimeMillis() - awaitStartTime
                    debugLog { "processSchedule() workJobs.awaitAll() RETURNED after ${awaitElapsed}ms" }
                    ScheduleStats(
                        backedUpCount = backedUpCount.get(),
                        skippedCount = skippedCount.get(),
                        totalSize = totalBackupSize.get()
                    )
                } else {
                    debugLog { "processSchedule() beginSchedule returned false (duplicate), returning null" }
                    endSchedule(name, "duplicate detected")
                    null
                }
            } else {
                debugLog { "processSchedule() worksList is empty, returning null" }
                beginSchedule(name, "no work")
                endSchedule(name, "no work")
                null
            }
        }

    private suspend fun getFilteredPackages(schedule: Schedule): List<String> {
        return withContext(Dispatchers.IO) {
            try {
                //FileUtils.ensureBackups()
                // TODO follow this down the rabbit hole to clean up the logic
                FileUtils.invalidateBackupLocation()
                NeoApp.startup = false

                val customBlocklist = schedule.blockList.toPersistentSet()
                val globalBlocklist = blocklistRepo.loadGlobalBlocklistOf().toPersistentSet()
                val blockList = globalBlocklist.plus(customBlocklist)
                val tagsMap = appExtrasRepo.getAll().associate { it.packageName to it.customTags }
                val allTags = appExtrasRepo.getAll().flatMap { it.customTags }.distinct()
                val tagsList = schedule.tagsList.filter { it in allTags }.toPersistentSet()

                val unfilteredPackages = context.getInstalledPackageList()

                filterPackages(
                    packages = unfilteredPackages,
                    tagsMap = tagsMap,
                    filter = schedule.filter,
                    specialFilter = schedule.specialFilter,
                    customList = schedule.customList,
                    blockList = blockList,
                    tagsList = tagsList,
                ).map { it.packageName }

            } catch (e: FileUtils.BackupLocationInAccessibleException) {
                Timber.e("Schedule failed: ${e.message}")
                emptyList()
            } catch (e: StorageLocationNotConfiguredException) {
                Timber.e("Schedule failed: ${e.message}")
                emptyList()
            }
        }
    }

    private fun handleEmptySelectedItems(name: String) {
        beginSchedule(name, "no work")
        endSchedule(name, "no work")
        showNotification(
            context,
            NeoActivity::class.java,
            notificationId,
            context.getString(R.string.schedule_failed),
            context.getString(R.string.empty_filtered_list),
            false
        )
        traceSchedule { "[$scheduleId] no packages matching" }
    }

    private fun beginSchedule(name: String, details: String = ""): Boolean {
        return if (NeoApp.runningSchedules[scheduleId] != true) {
            NeoApp.runningSchedules[scheduleId] = true
            NeoApp.beginLogSection("schedule $name")
            true
        } else {
            val message =
                "duplicate schedule detected: id=$scheduleId name='$name' (late, ignored) $details"
            Timber.w(message)
            if (pref_autoLogSuspicious.value)
                textLog(
                    listOf(
                        message,
                        "--- autoLogAfterSchedule $scheduleId $name${if (details.isEmpty()) "" else " ($details)"}"
                    ) + supportInfo()
                )
            false
        }
    }

    private fun endSchedule(name: String, details: String = "") {
        if (NeoApp.runningSchedules[scheduleId] != null) {
            NeoApp.runningSchedules.remove(scheduleId)
            if (pref_autoLogAfterSchedule.value) {
                textLog(
                    listOf(
                        "--- autoLogAfterSchedule id=$scheduleId name=$name${if (details.isEmpty()) "" else " ($details)"}"
                    ) + supportInfo()
                )
            }
            NeoApp.endLogSection("schedule $name")
        } else {
            traceSchedule { "[$scheduleId] duplicate schedule end: name='$name'${if (details.isEmpty()) "" else " ($details)"}" }
        }
    }

    @Synchronized
    private fun createForegroundNotification(): Notification {
        debugLog { "[NOTIF-CREATE] ScheduleWork.createForegroundNotification() ENTRY: scheduleId=$scheduleId, notificationId=$notificationId, cached=${notification != null}\n${getDebugStackTrace()}" }
        if (notification != null) {
            debugLog { "[NOTIF-CREATE] ScheduleWork.createForegroundNotification() returning CACHED notification: scheduleId=$scheduleId" }
            return notification!!
        }

        if (pref_useForegroundInService.value) {
            createNotificationChannel()
        }

        val contentPendingIntent = PendingIntent.getActivity(
            context,
            0,
            Intent(context, NeoActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )

        val cancelIntent = Intent(context, CommandReceiver::class.java).apply {
            action = ACTION_CANCEL_SCHEDULE
            putExtra(EXTRA_SCHEDULE_ID, scheduleId)
            putExtra(EXTRA_PERIODIC, inputData.getBoolean(EXTRA_PERIODIC, false))
        }
        val cancelPendingIntent = PendingIntent.getBroadcast(
            context,
            0,
            cancelIntent,
            PendingIntent.FLAG_IMMUTABLE
        )

        debugLog { "[NOTIF-CREATE] ScheduleWork.createForegroundNotification() building NEW notification: scheduleId=$scheduleId, channel=$CHANNEL_ID" }
        return NotificationCompat.Builder(context, CHANNEL_ID)
            .setContentTitle(context.getString(R.string.sched_notificationMessage))
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setOngoing(true)
            .setSilent(true)
            .setContentIntent(contentPendingIntent)
            .setPriority(NotificationCompat.PRIORITY_MAX)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .addAction(
                R.drawable.ic_close,
                context.getString(R.string.dialogCancel),
                cancelPendingIntent
            )
            .build()
            .also {
                notification = it
                val title = it.extras?.getCharSequence("android.title")?.toString() ?: ""
                debugLog { "[NOTIF-CREATE] ScheduleWork.createForegroundNotification() notification built and cached: scheduleId=$scheduleId, hashCode=${it.hashCode()}, title='$title'" }
            }
    }

    private fun createNotificationChannel() {
        val notificationManager =
            context.getSystemService(Context.NOTIFICATION_SERVICE) as NotificationManager
        val notificationChannel = NotificationChannel(
            CHANNEL_ID,
            CHANNEL_ID,
            NotificationManager.IMPORTANCE_HIGH
        ).apply {
            enableVibration(true)
        }
        notificationManager.createNotificationChannel(notificationChannel)
    }

    companion object {
        private val CHANNEL_ID = ScheduleWork::class.java.name
        private const val SCHEDULE_ONETIME = "schedule_one_time_"
        private const val SCHEDULE_WORK = "schedule_work_"
        private val runningSchedules = ConcurrentHashMap<Long, Boolean>()

        fun enqueuePeriodic(schedule: Schedule, reschedule: Boolean = false) {
            if (!schedule.enabled) return
            val workManager = get<WorkManager>(WorkManager::class.java)

            val (timeToRun, initialDelay) = calcRuntimeDiff(schedule)

            val constraints = Constraints.Builder()
                .setRequiresCharging(false) // TODO implement pref for charging, battery, network
                .build()

            val scheduleWorkRequest = PeriodicWorkRequestBuilder<ScheduleWork>(
                schedule.interval.toLong(),
                TimeUnit.DAYS,
            )
                .setInitialDelay(
                    initialDelay,
                    TimeUnit.MILLISECONDS,
                )
                //.setConstraints(constraints)
                .addTag("schedule_periodic_${schedule.id}")
                .setInputData(
                    workDataOf(
                        EXTRA_SCHEDULE_ID to schedule.id,
                        EXTRA_NAME to schedule.name,
                        EXTRA_PERIODIC to true,
                    )
                )
                .build()

            workManager.enqueueUniquePeriodicWork(
                "$SCHEDULE_WORK${schedule.id}",
                if (reschedule) ExistingPeriodicWorkPolicy.CANCEL_AND_REENQUEUE
                else ExistingPeriodicWorkPolicy.UPDATE,
                scheduleWorkRequest,
            )
            traceSchedule {
                "[${schedule.id}] schedule starting in: ${
                    TimeUnit.MILLISECONDS.toMinutes(
                        timeToRun - SystemUtils.now
                    )
                } minutes name=${schedule.name}"
            }
        }

        fun enqueueImmediate(schedule: Schedule) =
            enqueueOnce(schedule.id, schedule.name, false)

        fun enqueueScheduled(scheduleId: Long, scheduleName: String) =
            enqueueOnce(scheduleId, scheduleName, true)

        private fun enqueueOnce(scheduleId: Long, scheduleName: String, periodic: Boolean) {
            val scheduleWorkRequest = OneTimeWorkRequestBuilder<ScheduleWork>()
                .setInputData(
                    workDataOf(
                        EXTRA_SCHEDULE_ID to scheduleId,
                        EXTRA_NAME to scheduleName,
                        EXTRA_PERIODIC to periodic,
                    )
                )
                .setExpedited(OutOfQuotaPolicy.RUN_AS_NON_EXPEDITED_WORK_REQUEST)
                .addTag("schedule_${scheduleId}")
                .build()

            get<WorkManager>(WorkManager::class.java).enqueueUniqueWork(
                "${if (periodic) SCHEDULE_WORK else SCHEDULE_ONETIME}$scheduleId",
                ExistingWorkPolicy.REPLACE,
                scheduleWorkRequest,
            )
            traceSchedule {
                "[${scheduleId}] schedule starting immediately, name=${scheduleName}"
            }
        }

        // TODO use when periodic runs are fixed
        suspend fun scheduleAll() = coroutineScope {
            val scheduleRepo = get<ScheduleRepository>(ScheduleRepository::class.java)
            val workManager = get<WorkManager>(WorkManager::class.java)
            scheduleRepo.getAll().forEach { schedule ->
                val scheduleAlreadyRuns = runningSchedules[schedule.id] == true
                val scheduled = workManager
                    .getWorkInfosForUniqueWork("$SCHEDULE_WORK${schedule.id}")
                    .get().any { !it.state.isFinished }
                when {
                    scheduleAlreadyRuns || scheduled -> {
                        traceSchedule { "[${schedule.id}]: ignore $schedule" }
                    }

                    schedule.enabled                 -> {
                        traceSchedule { "[${schedule.id}]: enable $schedule" }
                        enqueuePeriodic(schedule, false)
                    }

                    else                             -> {
                        traceSchedule { "[${schedule.id}]: cancel $schedule" }
                        cancel(schedule.id, true)
                    }
                }
            }
            workManager.pruneWork()
        }

        fun cancel(scheduleId: Long, periodic: Boolean = true) {
            traceSchedule { "[$scheduleId]: Canceling" }
            get<WorkManager>(WorkManager::class.java)
                .cancelUniqueWork(
                    "${if (periodic) SCHEDULE_WORK else SCHEDULE_ONETIME}$scheduleId"
                )
        }
    }
}