package com.machiav3lli.backup.utils.extensions

import com.machiav3lli.backup.manager.handler.debugLog
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.launch
import timber.log.Timber

fun <T> Flow<T>.takeUntilSignal(signal: Flow<Boolean>): Flow<T> = channelFlow {
    val signalJob = launch {
        signal.distinctUntilChanged()
            .collect { if (it) close() } // Close the channel on signal
    }
    try {
        collect { send(it) }
    } catch (e: Exception) {
        debugLog { "takeUntilSignal: flow collector threw exception: ${e.javaClass.simpleName}: ${e.message}" }
        Timber.w(e, "Flow collector threw exception, flow terminated")
    } finally {
        signalJob.cancel()
    }
}