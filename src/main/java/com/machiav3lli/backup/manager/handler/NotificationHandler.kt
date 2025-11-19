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
package com.machiav3lli.backup.manager.handler

import android.annotation.SuppressLint
import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import com.machiav3lli.backup.R
import com.machiav3lli.backup.ui.activities.BaseActivity
import com.machiav3lli.backup.classAddress
import com.machiav3lli.backup.utils.SystemUtils

fun showNotification(
    context: Context?,
    parentActivity: Class<out BaseActivity?>?,
    id: Int, title: String?, text: String?,
    autoCancel: Boolean
) {
    showNotification(context, parentActivity, id, title, text, "", autoCancel)
}

@SuppressLint("MissingPermission")
fun showNotification(
    context: Context?,
    parentActivity: Class<out BaseActivity?>?,
    id: Int, title: String?, text: String?,
    bigText: String,
    autoCancel: Boolean
) {
    debugLog { "[NOTIF-CREATE] showNotification() ENTRY: id=$id, title='$title', text='$text', autoCancel=$autoCancel\n${getDebugStackTrace()}" }
    val resultIntent = Intent(context, parentActivity)
    resultIntent.flags = Intent.FLAG_ACTIVITY_SINGLE_TOP
    val resultPendingIntent = PendingIntent.getActivity(
        context, 0, resultIntent,
        PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT
    )
    val notificationChannel = NotificationChannel(
        classAddress("NotificationHandler"),
        classAddress("NotificationHandler"),
        NotificationManager.IMPORTANCE_LOW
    )
    val notificationManager = NotificationManagerCompat.from(context!!)
    notificationManager.createNotificationChannel(notificationChannel)
    debugLog { "[NOTIF-CREATE] showNotification() creating notification: id=$id, channel=${classAddress("NotificationHandler")}" }
    val notification = NotificationCompat.Builder(context, classAddress("NotificationHandler"))
        .setGroup(SystemUtils.packageName)
        .setSortKey("9")
        .setPriority(NotificationCompat.PRIORITY_DEFAULT)
        .setSmallIcon(R.drawable.ic_launcher_foreground)
        .setContentTitle(title)
        .setStyle(
            if (bigText.isEmpty()) null
            else NotificationCompat.BigTextStyle().bigText(bigText)
        )
        .setContentText(if (text.isNullOrEmpty()) null else text)
        .setAutoCancel(autoCancel)
        .setContentIntent(resultPendingIntent)
        .build()
    debugLog { "[NOTIF-POST] showNotification() posting notification: id=$id\n${getDebugStackTrace()}" }
    notificationManager.notify(id, notification)
    debugLog { "[NOTIF-POST] showNotification() notification posted successfully: id=$id" }
}
