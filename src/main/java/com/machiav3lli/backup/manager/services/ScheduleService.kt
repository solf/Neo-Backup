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
package com.machiav3lli.backup.manager.services

import android.app.Notification
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.Context
import android.content.Intent
import android.os.Build
import android.os.IBinder
import android.os.Process
import androidx.core.app.NotificationCompat
import androidx.work.WorkManager
import com.machiav3lli.backup.ACTION_CANCEL
import com.machiav3lli.backup.ACTION_RUN_SCHEDULE
import com.machiav3lli.backup.EXTRA_NAME
import com.machiav3lli.backup.EXTRA_SCHEDULE_ID
import com.machiav3lli.backup.NeoApp
import com.machiav3lli.backup.R
import com.machiav3lli.backup.data.dbs.repository.ScheduleRepository
import com.machiav3lli.backup.data.preferences.traceSchedule
import com.machiav3lli.backup.manager.handler.debugLog
import com.machiav3lli.backup.manager.handler.generateUniqueNotificationId
import com.machiav3lli.backup.manager.handler.getCompactStackTrace
import com.machiav3lli.backup.manager.handler.showNotification
import com.machiav3lli.backup.manager.tasks.ScheduleWork
import com.machiav3lli.backup.ui.activities.NeoActivity
import com.machiav3lli.backup.ui.pages.pref_fakeScheduleDups
import com.machiav3lli.backup.ui.pages.pref_useForegroundInService
import com.machiav3lli.backup.utils.SystemUtils
import com.machiav3lli.backup.utils.cancelScheduleAlarm
import com.machiav3lli.backup.utils.extensions.Android
import com.machiav3lli.backup.utils.scheduleAlarmsOnce
import com.machiav3lli.backup.utils.scheduleNextAlarm
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.koin.java.KoinJavaComponent.get
import timber.log.Timber

open class ScheduleService : Service() {
    lateinit var notification: Notification
    private var notificationId = -1

    override fun onBind(intent: Intent?): IBinder? = null

    override fun onCreate() {
        debugLog { "ScheduleService.onCreate() ENTRY: PID=${Process.myPid()}" }
        NeoApp.wakelock(true)
        traceSchedule { "%%%%% ############################################################ ScheduleService create" }
        super.onCreate()
        this.notificationId = generateUniqueNotificationId()

        val useForeground = pref_useForegroundInService.value
        debugLog { "ScheduleService.onCreate() useForegroundInService=$useForeground, notificationId=$notificationId" }
        if (useForeground) {
            createNotificationChannel()
            createForegroundInfo()
            val title = notification.extras?.getCharSequence("android.title")?.toString() ?: ""
            debugLog { "[NOTIF-FOREGROUND] ScheduleService.onCreate() calling startForeground: id=${notification.hashCode()}, notificationId=$notificationId, title='$title' | ${getCompactStackTrace()}" }
            startForeground(notification.hashCode(), this.notification)
            debugLog { "[NOTIF-FOREGROUND] ScheduleService.onCreate() started as FOREGROUND service: id=${notification.hashCode()}, title='$title'" }
        }

        debugLog { "ScheduleService.onCreate() EXIT: created notification" }
    }

    override fun onDestroy() {
        debugLog { "ScheduleService.onDestroy() ENTRY: service being destroyed" }
        traceSchedule { "%%%%% ############################################################ ScheduleService destroy" }
        NeoApp.wakelock(false)
        super.onDestroy()
        debugLog { "ScheduleService.onDestroy() EXIT" }
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val scheduleId = intent?.getLongExtra(EXTRA_SCHEDULE_ID, -1L) ?: -1L
        val scheduleName = intent?.getStringExtra(EXTRA_NAME) ?: ""
        val action = intent?.action

        debugLog { "ScheduleService.onStartCommand() ENTRY: scheduleId=$scheduleId, name='$scheduleName', startId=$startId, action=$action" }

        NeoApp.wakelock(true)

        traceSchedule {
            var message =
                "[$scheduleId] %%%%% ############################################################ ScheduleService startId=$startId PID=${Process.myPid()} starting for name='$scheduleName'"
            if (Android.minSDK(Build.VERSION_CODES.S)) {
                message += " ui=$isUiContext"
            }
            if (Android.minSDK(Build.VERSION_CODES.Q)) {
                message += " fgsv=$foregroundServiceType"
            }
            message
        }

        if (intent != null) {
            when (action) {
                ACTION_CANCEL       -> {
                    debugLog { "ScheduleService.onStartCommand() ACTION_CANCEL: calling stopSelf()" }
                    traceSchedule { "[$scheduleId] name='$scheduleName' action=$action" }
                    ScheduleWork.cancel(scheduleId)
                    NeoApp.wakelock(false)
                    traceSchedule { "%%%%% service stop" }
                    stopSelf()
                }

                ACTION_RUN_SCHEDULE -> {
                    debugLog { "ScheduleService.onStartCommand() ACTION_RUN_SCHEDULE" }
                    // scheduleId already read from extras
                    traceSchedule { "[$scheduleId] name='$scheduleName' action=$action" }
                }

                null                -> {
                    debugLog { "ScheduleService.onStartCommand() action=null, standard action" }
                    // no action = standard action, simply continue with extra data
                }

                else                -> {
                    debugLog { "ScheduleService.onStartCommand() unknown action=$action" }
                    traceSchedule { "[$scheduleId] name='$scheduleName' action=$action unknown, ignored" }
                }
            }
        }

        if (scheduleId >= 0) {
            debugLog { "ScheduleService.onStartCommand() checking for duplicate work: scheduleId=$scheduleId, name='$scheduleName'" }
            val workManager = get<WorkManager>(WorkManager::class.java)
            
            // Check if work is already queued/running in WorkManager
            // Need to check both periodic and one-time work names
            val periodicWorkName = "${ScheduleWork.SCHEDULE_WORK}$scheduleId"
            val oneTimeWorkName = "${ScheduleWork.SCHEDULE_ONETIME}$scheduleId"
            
            debugLog { "ScheduleService.onStartCommand() querying WorkManager for existing work: periodic='$periodicWorkName', oneTime='$oneTimeWorkName'" }
            
            val periodicWorkInfos = workManager.getWorkInfosForUniqueWork(periodicWorkName).get()
            val oneTimeWorkInfos = workManager.getWorkInfosForUniqueWork(oneTimeWorkName).get()
            
            debugLog { "ScheduleService.onStartCommand() WorkManager query results: periodicCount=${periodicWorkInfos.size}, oneTimeCount=${oneTimeWorkInfos.size}" }
            
            val periodicWorkQueued = periodicWorkInfos.any { !it.state.isFinished }
            val oneTimeWorkQueued = oneTimeWorkInfos.any { !it.state.isFinished }
            
            debugLog { "ScheduleService.onStartCommand() unfinished work check: periodicQueued=$periodicWorkQueued, oneTimeQueued=$oneTimeWorkQueued" }
            
            if (periodicWorkQueued || oneTimeWorkQueued) {
                val workType = if (periodicWorkQueued) "periodic" else "one-time"
                val workStates = (periodicWorkInfos + oneTimeWorkInfos)
                    .filter { !it.state.isFinished }
                    .map { it.state.name }
                    .joinToString(", ")
                
                debugLog { "ScheduleService.onStartCommand() DUPLICATE DETECTED: scheduleId=$scheduleId, workType=$workType, states=[$workStates]" }
                traceSchedule { 
                    "[$scheduleId] '$scheduleName' $workType work already queued/running in WorkManager (states: $workStates), skipping duplicate enqueue" 
                }
                Timber.i("[$scheduleId] Duplicate schedule prevented by ScheduleService WorkManager check: $workType work in states [$workStates]")
                
                // Still schedule next alarm for future runs
                debugLog { "ScheduleService.onStartCommand() duplicate case: scheduling next alarm" }
                scheduleAlarmsOnce(this)
                
                // Shutdown service after brief delay
                debugLog { "ScheduleService.onStartCommand() duplicate case: scheduling service shutdown" }
                CoroutineScope(Dispatchers.IO).launch {
                    delay(1000)  // Brief delay
                    debugLog { "ScheduleService duplicate case: calling stopSelf() after delay" }
                    stopSelf()
                }
                NeoApp.wakelock(false)
                debugLog { "ScheduleService.onStartCommand() duplicate case: EXIT, returning START_NOT_STICKY" }
                return START_NOT_STICKY
            }
            
            // Safe to enqueue - no existing work found
            debugLog { "ScheduleService.onStartCommand() no duplicate found, proceeding with enqueue: scheduleId=$scheduleId" }
            val repeatCount = 1 + pref_fakeScheduleDups.value
            debugLog { "ScheduleService.onStartCommand() repeatCount=$repeatCount (pref_fakeScheduleDups=${pref_fakeScheduleDups.value})" }
            repeat(repeatCount) { count ->
                debugLog { "ScheduleService.onStartCommand() repeat iteration $count: enqueueing schedule" }
                CoroutineScope(Dispatchers.IO).launch {
                    scheduleNextAlarm(this@ScheduleService, scheduleId, true)
                }
                ScheduleWork.enqueueScheduled(scheduleId, scheduleName)
                traceSchedule { "[$scheduleId] starting task for schedule${if (count > 0) " (dup $count)" else ""}" }
            }
        }

        scheduleAlarmsOnce(this)

        // Schedule service shutdown after giving WorkManager time to start
        // WorkManager's expedited work typically starts within 1-2 seconds
        // We wait 5 seconds to be safe, ensuring:
        // - WorkManager has started ScheduleWork
        // - ScheduleWork has acquired its own wakelock
        // - Safe to release service's wakelock reference
        // This prevents wakelock leaks while avoiding complex coordination
        CoroutineScope(Dispatchers.IO).launch {
            delay(5000)  // 5 second grace period
            debugLog { "ScheduleService delayed shutdown: calling stopSelf() after grace period" }
            stopSelf()
        }

        NeoApp.wakelock(false)
        debugLog { "ScheduleService.onStartCommand() EXIT: returning START_NOT_STICKY" }
        return START_NOT_STICKY
    }

    private fun createForegroundInfo() {
        debugLog { "[NOTIF-CREATE] ScheduleService.createForegroundInfo() ENTRY: notificationId=$notificationId | ${getCompactStackTrace()}" }
        val contentPendingIntent = PendingIntent.getActivity(
            this,
            0,
            Intent(this, NeoActivity::class.java),
            PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
        )
        val cancelIntent = Intent(this, ScheduleService::class.java).apply {
            action = ACTION_CANCEL
        }
        val cancelPendingIntent = PendingIntent.getBroadcast(
            this,
            0,
            cancelIntent,
            PendingIntent.FLAG_IMMUTABLE
        )
        debugLog { "[NOTIF-CREATE] ScheduleService.createForegroundInfo() building notification: channel=$CHANNEL_ID" }
        this.notification = NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle(getString(R.string.sched_starting_message))
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setOngoing(true)
            .setSilent(true)
            .setContentIntent(contentPendingIntent)
            .setPriority(NotificationCompat.PRIORITY_DEFAULT)
            .setCategory(NotificationCompat.CATEGORY_SERVICE)
            .addAction(R.drawable.ic_close, getString(R.string.dialogCancel), cancelPendingIntent)
            .build()
        val title = this.notification.extras?.getCharSequence("android.title")?.toString() ?: ""
        debugLog { "[NOTIF-CREATE] ScheduleService.createForegroundInfo() notification created: hashCode=${this.notification.hashCode()}, title='$title'" }
    }

    open fun createNotificationChannel() {
        val notificationManager = getSystemService(NOTIFICATION_SERVICE) as NotificationManager
        val notificationChannel =
            NotificationChannel(CHANNEL_ID, CHANNEL_ID, NotificationManager.IMPORTANCE_HIGH)
        notificationChannel.enableVibration(true)
        notificationManager.createNotificationChannel(notificationChannel)
    }

    companion object {
        private val CHANNEL_ID = ScheduleService::class.java.name

        suspend fun scheduleAll(context: Context) = coroutineScope {
            val scheduleRepo = get<ScheduleRepository>(ScheduleRepository::class.java)

            scheduleRepo.getAll().forEach { schedule ->
                when {
                    !schedule.enabled -> cancelScheduleAlarm(context, schedule.id, schedule.name)
                    else              -> scheduleNextAlarm(context, schedule.id, false)
                }
            }
        }
    }
}