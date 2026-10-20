# Standalone Windows API compatibility probe. A successful flush here does not
# demonstrate persistence across power loss or qualify a storage device.
[CmdletBinding()]
param(
    [string]$ParentDirectory = [System.IO.Path]::GetTempPath(),
    [switch]$KeepArtifacts
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$report = [ordered]@{
    schema_version = 1
    claim = 'Windows directory-open/flush API compatibility only; no power-loss durability claim'
    os = [System.Environment]::OSVersion.VersionString
    dotnet = [System.Environment]::Version.ToString()
    powershell = $PSVersionTable.PSVersion.ToString()
    required_variant = 'generic_write'
    required_variant_passed = $false
    artifact_directory = $null
    cases = @()
    fatal_error = $null
    cleanup_error = $null
}
$ownedDirectory = $null
$createdOwnedDirectory = $false
$exitCode = 1

try {
    if ([System.Environment]::OSVersion.Platform -ne [System.PlatformID]::Win32NT) {
        throw 'This probe requires Windows; no compatibility result was produced.'
    }
    $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent()
    try {
        $principal = [System.Security.Principal.WindowsPrincipal]::new($identity)
        $report['administrator_token'] = $principal.IsInRole(
            [System.Security.Principal.WindowsBuiltInRole]::Administrator)
    } finally {
        $identity.Dispose()
    }

    Add-Type -TypeDefinition @'
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

public static class DirectoryFlushCompatibilityProbe
{
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, ExactSpelling = true)]
    private static extern SafeFileHandle CreateFileW(string name, uint access,
        uint sharing, IntPtr security, uint disposition, uint flags, IntPtr template);
    [DllImport("kernel32.dll", SetLastError = true, ExactSpelling = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool FlushFileBuffers(SafeFileHandle handle);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, ExactSpelling = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetVolumeInformationByHandleW(SafeFileHandle handle,
        StringBuilder label, uint labelLength, out uint serial, out uint maxComponent,
        out uint flags, StringBuilder filesystem, uint filesystemLength);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, ExactSpelling = true)]
    private static extern uint GetFinalPathNameByHandleW(SafeFileHandle handle,
        StringBuilder path, uint length, uint flags);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, ExactSpelling = true)]
    private static extern uint GetDriveTypeW(string root);

    public static Dictionary<string, object> Run(string path, uint access)
    {
        const uint share = 7; // FILE_SHARE_READ | WRITE | DELETE
        const uint openExisting = 3;
        const uint backupSemantics = 0x02000000;
        var result = new Dictionary<string, object>();
        result["path"] = path;
        result["desired_access"] = "0x" + access.ToString("X8");
        result["share_mode"] = "0x00000007";
        result["creation_disposition"] = "OPEN_EXISTING";
        result["flags"] = "0x02000000 FILE_FLAG_BACKUP_SEMANTICS";
        result["open_ok"] = false;
        result["flush_attempted"] = false;
        result["flush_ok"] = false;
        result["local_ntfs_candidate"] = false;
        var total = Stopwatch.StartNew();
        using (var handle = CreateFileW(path, access, share, IntPtr.Zero,
            openExisting, backupSemantics, IntPtr.Zero))
        {
            int openError = Marshal.GetLastWin32Error();
            result["open_error"] = handle.IsInvalid ? (object)openError : null;
            if (handle.IsInvalid)
            {
                result["open_error_message"] = new Win32Exception(openError).Message;
                result["total_elapsed_ms"] = total.Elapsed.TotalMilliseconds;
                return result;
            }
            result["open_ok"] = true;

            // Measure the exact flush before any diagnostic query. There is no
            // retry, alternate access mode, or successful fallback in this case.
            var flush = Stopwatch.StartNew();
            bool flushOk = FlushFileBuffers(handle);
            int flushError = Marshal.GetLastWin32Error();
            flush.Stop();
            result["flush_attempted"] = true;
            result["flush_ok"] = flushOk;
            result["flush_error"] = flushOk ? null : (object)flushError;
            result["flush_error_message"] = flushOk ? null : new Win32Exception(flushError).Message;
            result["flush_elapsed_ms"] = flush.Elapsed.TotalMilliseconds;

            uint serial, maxComponent, filesystemFlags;
            var label = new StringBuilder(261);
            var filesystem = new StringBuilder(261);
            bool volumeOk = GetVolumeInformationByHandleW(handle, label, 261,
                out serial, out maxComponent, out filesystemFlags, filesystem, 261);
            int volumeError = Marshal.GetLastWin32Error();
            result["volume_query_ok"] = volumeOk;
            result["volume_query_error"] = volumeOk ? null : (object)volumeError;
            if (volumeOk)
            {
                result["filesystem"] = filesystem.ToString();
                result["volume_label"] = label.ToString();
                result["volume_serial"] = "0x" + serial.ToString("X8");
                result["filesystem_flags"] = "0x" + filesystemFlags.ToString("X8");
                result["maximum_component_length"] = maxComponent;
            }

            // VOLUME_NAME_GUID avoids inferring locality from a drive letter,
            // which could name a mapping or traverse another mounted volume.
            var finalPath = new StringBuilder(32768);
            uint finalLength = GetFinalPathNameByHandleW(handle, finalPath, 32768, 1);
            int finalError = Marshal.GetLastWin32Error();
            bool finalOk = finalLength > 0 && finalLength < 32768;
            result["final_path_query_ok"] = finalOk;
            result["final_path_query_error"] = finalLength == 0 ? (object)finalError : null;
            result["final_path_required_length"] = finalLength;
            if (finalOk)
            {
                string final = finalPath.ToString();
                result["final_guid_path"] = final;
                int rootEnd = final.IndexOf(@"}\", StringComparison.Ordinal);
                if (final.StartsWith(@"\\?\Volume{", StringComparison.OrdinalIgnoreCase)
                    && rootEnd >= 0)
                {
                    string root = final.Substring(0, rootEnd + 2);
                    uint driveType = GetDriveTypeW(root);
                    result["volume_guid_root"] = root;
                    result["drive_type"] = driveType;
                    result["local_ntfs_candidate"] = volumeOk && driveType == 3
                        && String.Equals(filesystem.ToString(), "NTFS", StringComparison.OrdinalIgnoreCase);
                }
            }
        }
        result["total_elapsed_ms"] = total.Elapsed.TotalMilliseconds;
        return result;
    }
}
'@

    $parent = [System.IO.Path]::GetFullPath($ParentDirectory)
    if (-not [System.IO.Directory]::Exists($parent)) {
        throw 'ParentDirectory must already exist; the probe does not create arbitrary ancestors.'
    }
    $ownedDirectory = [System.IO.Path]::Combine($parent,
        'cryptofeed-directory-flush-probe-' + [System.Guid]::NewGuid().ToString('N'))
    if ([System.IO.Directory]::Exists($ownedDirectory) -or [System.IO.File]::Exists($ownedDirectory)) {
        throw 'Refusing a pre-existing probe artifact path.'
    }
    [void][System.IO.Directory]::CreateDirectory($ownedDirectory)
    $createdOwnedDirectory = $true
    $report['artifact_directory'] = $ownedDirectory

    $variants = @(
        @{ name = 'generic_write'; access = [uint32]1073741824 },
        @{ name = 'generic_read_write_diagnostic'; access = [uint32]3221225472 },
        @{ name = 'generic_read_control'; access = [uint32]2147483648 }
    )
    foreach ($variant in $variants) {
        $caseDirectory = [System.IO.Path]::Combine($ownedDirectory, $variant.name)
        [void][System.IO.Directory]::CreateDirectory($caseDirectory)
        $sentinel = [System.IO.Path]::Combine($caseDirectory, 'sentinel.bin')
        $stream = [System.IO.FileStream]::new($sentinel,
            [System.IO.FileMode]::CreateNew, [System.IO.FileAccess]::ReadWrite,
            ([System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete))
        try {
            $bytes = [System.Text.Encoding]::UTF8.GetBytes('directory-flush-compatibility-probe')
            $stream.Write($bytes, 0, $bytes.Length)
            $stream.Flush($true)
            # Keep a regular file open with the same sharing as live readers.
            $case = [DirectoryFlushCompatibilityProbe]::Run($caseDirectory, $variant.access)
        } finally {
            $stream.Dispose()
        }
        $case['name'] = $variant.name
        $report['cases'] += $case
        if ($variant.name -eq 'generic_write') {
            $report['required_variant_passed'] = [bool]($case['open_ok'] -and
                $case['flush_ok'] -and $case['local_ntfs_candidate'])
        }
    }
    if ($report['required_variant_passed']) {
        $exitCode = 0
    }
} catch {
    $report['fatal_error'] = $_.Exception.ToString()
} finally {
    if ($createdOwnedDirectory -and -not $KeepArtifacts) {
        try {
            # The GUID path above is the only directory this script removes.
            [System.IO.Directory]::Delete($ownedDirectory, $true)
        } catch {
            $report['cleanup_error'] = $_.Exception.ToString()
            $exitCode = 1
        }
    }
    $report | ConvertTo-Json -Depth 8
}
exit $exitCode
