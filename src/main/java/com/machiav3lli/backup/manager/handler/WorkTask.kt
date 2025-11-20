package com.machiav3lli.backup.manager.handler

import androidx.work.Data
import androidx.work.WorkInfo
import androidx.work.WorkManager
import androidx.work.workDataOf
import java.util.UUID

/**
 * Wrapper for a single WorkManager task with clean query API
 */
class WorkTask(
    private val workManager: WorkManager,
    private val workRequestId: UUID
) {
    val id: UUID get() = workRequestId
    
    companion object {
        /**
         * Create a placeholder WorkInfo for when the actual WorkInfo is not available
         */
        private fun createErrorPlaceholder(workRequestId: UUID): WorkInfo {
            return WorkInfo(
                id = workRequestId,
                state = WorkInfo.State.FAILED,
                outputData = workDataOf(
                    "error" to "WorkInfo not available from WorkManager (task ID: $workRequestId)",
                    "packageName" to "",
                    "packageLabel" to "",
                    "backupSize" to 0L,
                    "backupBoolean" to false
                ),
                tags = emptySet(),
                progress = Data.EMPTY,
                runAttemptCount = 0,
                generation = 0
            )
        }
    }
    
    /**
     * Check if task has completed (succeeded, failed, or cancelled)
     */
    fun isCompleted(): Boolean {
        return getResult().state.isFinished
    }
    
    /**
     * Get full WorkInfo with state and output data
     * Returns error placeholder if WorkManager has lost/discarded the task info
     */
    fun getResult(): WorkInfo {
        return workManager.getWorkInfoById(workRequestId).get()
            ?: createErrorPlaceholder(workRequestId)
    }
    
    /**
     * Check if task succeeded
     */
    fun isSucceeded(): Boolean = getResult().state == WorkInfo.State.SUCCEEDED
    
    /**
     * Check if task failed permanently
     */
    fun isFailed(): Boolean = getResult().state == WorkInfo.State.FAILED
    
    /**
     * Check if task was cancelled
     */
    fun isCancelled(): Boolean = getResult().state == WorkInfo.State.CANCELLED
    
    /**
     * Get output data from completed task
     */
    fun getOutputData(): Data = getResult().outputData
    
    /**
     * Get backup size from output data (0 if not a backup or failed)
     */
    fun getBackupSize(): Long = getOutputData().getLong("backupSize", 0L)
    
    /**
     * Get error message from output data (empty if no error)
     */
    fun getError(): String = getOutputData().getString("error") ?: ""
    
    /**
     * Get package name from output data
     */
    fun getPackageName(): String = getOutputData().getString("packageName") ?: ""
    
    /**
     * Get package label from output data
     */
    fun getPackageLabel(): String = getOutputData().getString("packageLabel") ?: ""
    
    /**
     * Check if this was a backup operation (vs restore)
     */
    fun isBackup(): Boolean = getOutputData().getBoolean("backupBoolean", false)
    
    /**
     * Check if task was skipped (no changes)
     */
    fun isSkipped(): Boolean {
        val error = getError()
        return error.contains("Skipped", ignoreCase = true)
    }
}

