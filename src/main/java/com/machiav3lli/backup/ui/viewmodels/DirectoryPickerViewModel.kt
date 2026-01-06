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
package com.machiav3lli.backup.ui.viewmodels

import android.os.Environment
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.setValue
import androidx.lifecycle.ViewModel
import timber.log.Timber
import java.io.File

class DirectoryPickerViewModel : ViewModel() {
    
    var currentPath by mutableStateOf<File?>(null)
        private set
    
    var directories by mutableStateOf<List<File>>(emptyList())
        private set
    
    var errorMessage by mutableStateOf<String?>(null)
        private set
    
    val canGoUp: Boolean
        get() = currentPath?.parent != null
    
    init {
        navigateToDirectory(getStartingPath())
    }
    
    fun getStartingPath(): File {
        // Try /storage/emulated/0/ first
        val primaryStorage = File("/storage/emulated/0")
        if (primaryStorage.exists() && primaryStorage.canRead()) {
            return primaryStorage
        }
        
        // Fallback to external storage directory
        val externalStorage = Environment.getExternalStorageDirectory()
        if (externalStorage.exists() && externalStorage.canRead()) {
            return externalStorage
        }
        
        // Last resort - root storage
        return File("/storage")
    }
    
    fun navigateToDirectory(dir: File) {
        try {
            if (!dir.exists()) {
                errorMessage = "Directory does not exist: ${dir.path}"
                return
            }
            
            if (!dir.canRead()) {
                errorMessage = "Cannot read directory: ${dir.path}. Check permissions."
                return
            }
            
            currentPath = dir
            errorMessage = null
            listDirectories(dir)
        } catch (e: Exception) {
            Timber.e(e, "Failed to navigate to directory: ${dir.path}")
            errorMessage = "Error accessing directory: ${e.message}"
        }
    }
    
    fun navigateUp() {
        val parent = currentPath?.parentFile
        if (parent != null) {
            navigateToDirectory(parent)
        }
    }
    
    private fun listDirectories(path: File) {
        try {
            val files = path.listFiles { f ->
                f.isDirectory && f.canRead() && !f.name.startsWith(".")
            }
            
            directories = files?.sortedBy { it.name.lowercase() } ?: emptyList()
            
            if (directories.isEmpty()) {
                Timber.d("No subdirectories found in: ${path.path}")
            }
        } catch (e: SecurityException) {
            Timber.e(e, "Security exception listing directory: ${path.path}")
            errorMessage = "Permission denied: ${e.message}"
            directories = emptyList()
        } catch (e: Exception) {
            Timber.e(e, "Failed to list directory: ${path.path}")
            errorMessage = "Error listing directory: ${e.message}"
            directories = emptyList()
        }
    }
    
    fun clearError() {
        errorMessage = null
    }
}

