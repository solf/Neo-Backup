package com.machiav3lli.backup.utils

import android.content.Context
import android.content.Intent
import com.machiav3lli.backup.manager.services.PersistentOperationService

/**
 * Context for updating operation progress.
 */
class ProgressContext internal constructor(
    private val onUpdate: (String) -> Unit
) {
    /**
     * Update the notification with current status.
     */
    fun update(message: String) {
        onUpdate(message)
    }
}

/**
 * Run an operation persistently (survives backgrounding, not app kill).
 * Shows a foreground notification that updates with progress.
 * 
 * Usage:
 * ```
 * runPersistentOperation(context, "My Operation") { progress ->
 *     progress.update("Step 1")
 *     doSomething()
 *     progress.update("Step 2")
 *     doSomethingElse()
 *     progress.update("Done!")
 * }
 * ```
 * 
 * @param context Android context
 * @param title Title for the notification
 * @param operation Lambda to execute
 */
fun runPersistentOperation(
    context: Context,
    title: String,
    operation: suspend (progress: ProgressContext) -> Unit
) {
    // Generate unique ID for this operation
    val operationId = java.util.UUID.randomUUID().toString()
    
    // Store operation in thread-safe map
    PersistentOperationService.pendingOperations[operationId] = operation
    
    // Start foreground service with operation ID
    val intent = Intent(context, PersistentOperationService::class.java).apply {
        putExtra("title", title)
        putExtra("operationId", operationId)
    }
    
    context.startForegroundService(intent)
}

