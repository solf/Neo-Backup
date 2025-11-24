package com.machiav3lli.backup.manager.services

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.Service
import android.content.Intent
import android.os.IBinder
import androidx.core.app.NotificationCompat
import com.machiav3lli.backup.NeoApp
import com.machiav3lli.backup.R
import com.machiav3lli.backup.manager.handler.generateUniqueNotificationId
import com.machiav3lli.backup.manager.handler.showNotification
import com.machiav3lli.backup.ui.activities.NeoActivity
import com.machiav3lli.backup.utils.ProgressContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import timber.log.Timber
import java.util.concurrent.ConcurrentHashMap

/**
 * Generic foreground service that runs any operation persistently.
 * Survives app backgrounding but not app kill.
 */
class PersistentOperationService : Service() {
    
    private val job = Job()
    private val scope = CoroutineScope(Dispatchers.IO + job)
    private var notificationId = 0
    
    override fun onBind(intent: Intent?): IBinder? = null
    
    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        val title = intent?.getStringExtra("title") ?: "Operation"
        val operationId = intent?.getStringExtra("operationId")
        notificationId = generateUniqueNotificationId()
        
        if (operationId == null) {
            Timber.e("No operationId provided, stopping service")
            stopSelf()
            return START_NOT_STICKY
        }
        
        // Start as foreground immediately
        startForeground(notificationId, createNotification(title, "Starting..."))
        
        // Run the operation
        scope.launch {
            runOperation(title, operationId)
            stopSelf()
        }
        
        return START_NOT_STICKY
    }
    
    private suspend fun runOperation(title: String, operationId: String) {
        NeoApp.wakelock(true)
        
        try {
            // Retrieve and remove operation from map (thread-safe)
            val operation = pendingOperations.remove(operationId)
            if (operation == null) {
                Timber.w("No pending operation found for ID: $operationId")
                return
            }
            
            // Create progress context that updates notification
            val progress = ProgressContext { message ->
                showNotification(
                    this,
                    NeoActivity::class.java,
                    notificationId,
                    title,
                    message,
                    false
                )
            }
            
            // Run the user's operation
            operation(progress)
            
            // Final notification (dismissible)
            showNotification(
                this,
                NeoActivity::class.java,
                notificationId,
                title,
                getString(R.string.operation_completed_generic),
                true
            )
        } catch (e: Exception) {
            Timber.e(e, "Operation failed: $title")
            showNotification(
                this,
                NeoActivity::class.java,
                notificationId,
                title,
                getString(R.string.operation_failed),
                true
            )
        } finally {
            NeoApp.wakelock(false)
        }
    }
    
    private fun createNotification(title: String, message: String): android.app.Notification {
        val channel = NotificationChannel(
            CHANNEL_ID,
            "Background Operations",
            NotificationManager.IMPORTANCE_LOW
        )
        val notificationManager = getSystemService(NotificationManager::class.java)
        notificationManager.createNotificationChannel(channel)
        
        return NotificationCompat.Builder(this, CHANNEL_ID)
            .setContentTitle(title)
            .setContentText(message)
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setOngoing(true)
            .setSilent(true)
            .build()
    }
    
    override fun onDestroy() {
        job.cancel()
        super.onDestroy()
    }
    
    companion object {
        private const val CHANNEL_ID = "PersistentOperations"
        
        // Thread-safe map for storing operations by unique ID
        internal val pendingOperations = ConcurrentHashMap<String, suspend (ProgressContext) -> Unit>()
    }
}

