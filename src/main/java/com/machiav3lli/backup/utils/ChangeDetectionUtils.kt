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

import java.util.LinkedList

/**
 * Result of directory change detection scan
 * @param hasChanges whether any changes were detected
 * @param foundPath relative path where changes were found (null if no changes)
 * @param foundTimestamp timestamp of the changed file/directory (0 if no changes)
 */
data class ChangeDetectionResult(
    val hasChanges: Boolean,
    val foundPath: String? = null,
    val foundTimestamp: Long = 0L
)

object ChangeDetectionUtils {
    
    /**
     * Scans a directory tree for files/directories modified after a given timestamp.
     * Uses breadth-first search with early exit optimization.
     * Works with both regular File and RootFile (polymorphism via File base class).
     * 
     * @param rootDir the root directory to scan (File or RootFile)
     * @param thresholdTimestamp only report changes newer than this timestamp
     * @param maxDepth maximum depth to scan (0 = root only, 1 = root + immediate children, etc.)
     * @param hotPath optional hint - relative path to check first before full scan
     * @return ChangeDetectionResult with scan outcome
     */
    fun scanForChanges(
        rootDir: java.io.File,
        thresholdTimestamp: Long,
        maxDepth: Int,
        hotPath: String? = null
    ): ChangeDetectionResult {
        if (!rootDir.exists() || !rootDir.isDirectory) {
            return ChangeDetectionResult(hasChanges = false)
        }
        
        // If hot path provided, check it with two-step process (optimization)
        if (!hotPath.isNullOrEmpty()) {
            val result = checkHotPath(rootDir, hotPath, thresholdTimestamp)
            if (result.hasChanges) {
                return result
            }
            // Hot path didn't find changes, fall through to full BFS
        }
        
        // Breadth-first search with early exit
        return bfsSearchForChanges(rootDir, thresholdTimestamp, maxDepth)
    }
    
    /**
     * Two-step hot-path check for directories.
     * Step 1: Check directory timestamp (fast, optimistic)
     * Step 2: If old, list children and check their timestamps (still fast, reliable)
     * 
     * @return ChangeDetectionResult with hot-path as foundPath if change detected
     */
    private fun checkHotPath(
        rootDir: java.io.File,
        hotPath: String,
        thresholdTimestamp: Long
    ): ChangeDetectionResult {
        val hotDir = if (hotPath == ".") rootDir else java.io.File(rootDir, hotPath)
        
        if (!hotDir.exists() || !hotDir.isDirectory) {
            return ChangeDetectionResult(hasChanges = false)
        }
        
        // Step 1: Check directory timestamp (optimistic)
        val dirTimestamp = hotDir.lastModified()
        if (dirTimestamp > thresholdTimestamp) {
            return ChangeDetectionResult(
                hasChanges = true,
                foundPath = hotPath,
                foundTimestamp = dirTimestamp
            )
        }
        
        // Step 2: Directory timestamp is old, but changes might be inside
        // List children and check their timestamps (reliable fallback)
        try {
            val children = hotDir.listFiles()
            if (children != null) {
                for (child in children) {
                    val childTimestamp = child.lastModified()
                    if (childTimestamp > thresholdTimestamp) {
                        return ChangeDetectionResult(
                            hasChanges = true,
                            foundPath = hotPath,  // Still return hot-path, not child
                            foundTimestamp = childTimestamp
                        )
                    }
                }
            }
        } catch (e: Exception) {
            // Can't list directory, assume no changes
            return ChangeDetectionResult(hasChanges = false)
        }
        
        // No changes in hot-path
        return ChangeDetectionResult(hasChanges = false)
    }
    
    /**
     * Performs breadth-first search for changed files/directories.
     * Returns immediately upon finding the first change.
     * 
     * IMPORTANT: Returns the PARENT directory of the changed item as hot-path.
     * This allows Phase 1 to reliably check by listing one directory.
     */
    private fun bfsSearchForChanges(
        rootDir: java.io.File,
        thresholdTimestamp: Long,
        maxDepth: Int
    ): ChangeDetectionResult {
        // Queue stores (file, depth, parent)
        val queue = LinkedList<Triple<java.io.File, Int, java.io.File?>>()
        queue.add(Triple(rootDir, 0, null))  // Root has no parent
        
        while (queue.isNotEmpty()) {
            val (currentFile, currentDepth, parent) = queue.removeFirst()
            
            try {
                // Check if this file/directory was modified
                val lastModified = currentFile.lastModified()
                if (lastModified > thresholdTimestamp) {
                    // Found a change! Store PARENT directory as hot-path
                    val parentPath = if (parent == null) {
                        "."  // Changed at root, stay at root
                    } else {
                        parent.relativeTo(rootDir).path.ifEmpty { "." }
                    }
                    return ChangeDetectionResult(
                        hasChanges = true,
                        foundPath = parentPath,
                        foundTimestamp = lastModified
                    )
                }
                
                // If this is a directory and we haven't reached max depth, add children to queue
                if (currentFile.isDirectory && currentDepth < maxDepth) {
                    val children = currentFile.listFiles()
                    if (children != null) {
                        for (child in children) {
                            queue.add(Triple(child, currentDepth + 1, currentFile))
                        }
                    }
                }
            } catch (e: Exception) {
                // Skip files we can't access, continue scanning
                continue
            }
        }
        
        // No changes found
        return ChangeDetectionResult(hasChanges = false)
    }
}

