using Greenhouse.Common;
using Greenhouse.Common.Exceptions;
using Greenhouse.Data.Model.Core;
using Greenhouse.Data.Model.Setup;
using Greenhouse.Jobs.Infrastructure;
using Greenhouse.Logging;
using Greenhouse.Services.RemoteAccess;
using Greenhouse.Utilities;
using Newtonsoft.Json;
using NLog;
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;

namespace Greenhouse.Jobs.Framework
{
    [Export("GenericAggregateImportJob", typeof(IDragoJob))]
    public class AggregateImportJob : BaseFrameworkJob, IDragoJob
    {
        private static readonly Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        private RemoteAccessClient _rac;
        private List<IFile> _importFiles;
        private Uri _baseDestUri;
        private Constants.FileLogCheckType _fileLogCheckType;
        private int exceptionCounter;
        private int warningCounter;

        public void PreExecute()
        {
            Stage = Constants.ProcessingStage.RAW;
            base.Initialize();
            _baseDestUri = GetDestinationFolder();
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"IMPORT-PREEXECUTE {this.GetJobCacheKey()}")));
        }

        public void Execute()
        {
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"EXECUTE START {this.GetJobCacheKey()}")));

            //initialize the appropriate client for this integration
            _rac = GetRemoteAccessClient();
            RegexCodec regCod = new RegexCodec(CurrentIntegration.RegexMask);
            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid(
                $"Integration: {CurrentIntegration.IntegrationName}, fetching source files against regex: {regCod.FileNameRegex}. File Start Date is: {CurrentIntegration.FileStartDate}")));
            _importFiles = _rac.WithDirectory().GetFiles().Where(f =>
                    regCod.FileNameRegex.IsMatch(f.Name) && f.LastWriteTimeUtc >= CurrentIntegration.FileStartDate)
                .ToList();

            var sourceFileDict = new Dictionary<string, List<SourceFile>>();

            // if source files are uniquely named then we treat them all as a single batch
            // otherwise, if source files have the same name, then we group them
            // and the rest will be imported individually by date
            var overrideGroups = SourceFiles.GroupBy(x => x.SourceFileName)
                        .Where(group => group.Count() > 1);

            if (overrideGroups.Any())
            {
                var overrideGroupNames = overrideGroups.Select(x => x.Key);

                var sourceFileSingles = base.SourceFiles.Where(x => !overrideGroupNames.Contains(x.SourceFileName)).ToList();

                // add each source file to the dictionary in order to have it processed individually
                sourceFileSingles.ForEach(sf => sourceFileDict[sf.SourceFileName] = new List<SourceFile>() { sf });

                // add each override group to the dictionary to be processed together
                foreach (var group in overrideGroups)
                {
                    sourceFileDict[group.Key] = group.ToList();
                }
            }
            else
            {
                // if all source files uniquely named then we batch them together
                sourceFileDict["all"] = base.SourceFiles.ToList();
            }

            foreach (var sourceFileGroup in sourceFileDict)
            {
                var sourceFileList = sourceFileGroup.Value;

                if (sourceFileGroup.Key == "all")
                {
                    // only check if any remote files do not match when we use all source files
                    // if product is grouping only specific source files, then we cannot do this check against the entire bucket
                    // because the premise is we want to group these files only and don't care about other ones (subset of the files available)
                    var regexMissing = CheckRemoteFilesRegex(sourceFileList);

                    if (regexMissing)
                    {
                        var missingRegexExc = new RegexException($"Unable to find a matching source file regex for all remote files.");
                        exceptionCounter++;
                        _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid($"Import job error ->  Exception: {missingRegexExc.Message}")));
                        break;
                    }
                }

                // get matching files
                // if sourceFileGrouping is for all files in bucket, then we try and group all remotes files
                // otherwise, we use the regex for each sourcefile to target those specific files in s3
                var filesToImport = new List<IFile>();
                var matchingFiles = _importFiles.Where(x => sourceFileList.Any(s => s.FileRegexCodec.FileNameRegex.IsMatch(x.Name))).ToList();
                filesToImport.AddRange(matchingFiles);

                if (filesToImport.Count == 0)
                {
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, base.PrefixJobGuid($"Skipping Import-No matching remote files for source file group - {sourceFileGroup.Key}" +
                           $" - sourcefile ID:{string.Join(",", sourceFileList.Select(x => x.SourceFileID))}")));
                    continue;
                }

                var fileCollectionGroups = (sourceFileList.Count > 1) ? GetFileCollectionGroups(sourceFileList, filesToImport) : GetIndividualFilesByDate(sourceFileList, filesToImport);

                if (fileCollectionGroups?.Count() < 1 || fileCollectionGroups == null)
                {
                    _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                        PrefixJobGuid(
                            $"No files to import for source file group - {sourceFileGroup.Key} Integration: {CurrentIntegration.IntegrationName}( {CurrentIntegration.IntegrationID} )")));
                    continue;
                }

                ImportFileCollectionGroups(fileCollectionGroups, sourceFileList);
            }

            if (exceptionCounter > 0)
            {
                throw new ErrorsFoundException($"Total errors: {exceptionCounter}; Please check Splunk for more detail.");
            }
            else if (warningCounter > 0)
            {
                JobLogger.JobLog.Status = Constants.JobLogStatus.Warning.ToString();
                JobLogger.JobLog.Message = $"Total warnings: {warningCounter}; For full list search for Warnings in splunk";
            }

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name, PrefixJobGuid($"EXECUTE END {this.GetJobCacheKey()}")));
        }

        private IEnumerable<FileCollectionGroup> GetIndividualFilesByDate(List<SourceFile> sourceFileList, List<IFile> filesToImport)
        {
            // get processed files
            var processedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);

            // get what is missing
            var whatsMissing = filesToImport.Except(filesToImport.Where(s => processedFiles.Select(p => p.FileName).Contains(s.Name))).OrderBy(p => p.LastWriteTimeUtc).ToList();

            //Only import remote files not found in file log
            var remoteFilesGrouped = GetFileShellList(sourceFileList, whatsMissing);

            var remoteFileGroupToImport = remoteFilesGrouped.Select(gf => new FileCollectionGroup()
            {
                FileDate = gf.FileDate,
                DistinctFileTypeCount = 1,
                RemoteFiles = new List<IFile> { gf.RemoteFile }
            });

            return remoteFileGroupToImport;
        }

        /// <summary>
        /// File Shell consists of file attributes used to place commonalities among a file batch
        /// with the goal of putting those files into a file collection
        /// </summary>
        /// <param name="sourceFileList"></param>
        /// <param name="remoteGrouped"></param>
        /// <returns></returns>
        private static List<FileShell> GetFileShellList(List<SourceFile> sourceFileList, List<IFile> remoteGrouped)
        {
            var fileShellList = new List<FileShell>();

            var matchingFiles = remoteGrouped.Select(remoteFile =>
                {
                    var matchingSourceFile = sourceFileList.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(remoteFile.Name) && s.FileRegexCodec.TryParse(remoteFile.Name));
                    return new FileShell()
                    {
                        RemoteFile = remoteFile,
                        FileDate = matchingSourceFile.FileRegexCodec.FileNameDate ?? Greenhouse.Utilities.UtilsDate.TryParseExact($"{matchingSourceFile.FileRegexCodec.FileNameMonth}-01-{matchingSourceFile.FileRegexCodec.FileNameYear}"),
                        SourceFile = matchingSourceFile,
                        EntityID = matchingSourceFile.FileRegexCodec.EntityId,
                        FileDateHour = matchingSourceFile.FileRegexCodec.FileNameHour
                    };
                });

            fileShellList.AddRange(matchingFiles);

            return fileShellList;
        }

        /// <summary>
        /// File Shell consists of file attributes used to place commonalities among a file batch
        /// with the goal of putting those files into a file collection
        /// </summary>
        /// <param name="regexCodec"></param>
        /// <param name="remoteGrouped"></param>
        /// <returns></returns>
        private static List<FileShell> GetFileShellList(RegexCodec regexCodec, List<IFile> remoteGrouped)
        {
            var fileShellList = new List<FileShell>();

            var matchingFiles = remoteGrouped.Select(remoteFile =>
            {
                var matchingSourceFile = regexCodec.FileNameRegex.IsMatch(remoteFile.Name) && regexCodec.TryParse(remoteFile.Name);
                return new FileShell()
                {
                    RemoteFile = remoteFile,
                    FileDate = regexCodec.FileNameDate ?? Greenhouse.Utilities.UtilsDate.TryParseExact($"{regexCodec.FileNameMonth}-01-{regexCodec.FileNameYear}"),
                    EntityID = regexCodec.EntityId,
                    FileDateHour = regexCodec.FileNameHour
                };
            });

            fileShellList.AddRange(matchingFiles);

            return fileShellList;
        }

        private IEnumerable<FileCollectionGroup> GetFileCollectionGroups(List<SourceFile> sourceFileList, List<IFile> filesToImport)
        {
            RegexCodec regCod;
            IEnumerable<string> regexMaskGroupNames = Array.Empty<string>();

            var hasDoneFile = sourceFileList.Any(sf => sf.IsDoneFile);

            if (hasDoneFile)
            {
                _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                    PrefixJobGuid($"Done file flagged in Source File and its regex will be used to check the file log.")));

                var doneFile = sourceFileList.First(sf => sf.IsDoneFile);
                regCod = new RegexCodec(doneFile.RegexMask);
            }
            else
            {
                var sourceFile = sourceFileList.First();
                regCod = new RegexCodec(sourceFile.RegexMask);

                //Get all possible RegexMask group names from each source file type
                var regexMaskGroupNameLists = sourceFileList.Select(x => new RegexCodec(x.RegexMask))
                    .Select(xx => xx.FileNameRegex.GetGroupNames()).GroupBy(name => name)
                    .Select(group => group.First());

                //Get all common RegexMask group names and use that commonality to determine how to group files as a collection
                foreach (var list in regexMaskGroupNameLists)
                {
                    regexMaskGroupNames = regexMaskGroupNames.Any() ? regexMaskGroupNames : list;
                    regexMaskGroupNames = regexMaskGroupNames.Intersect(list);
                }
            }

            var remoteFileGroupToImport = GetFilesByCheckType(regCod, hasDoneFile, regexMaskGroupNames.ToArray(), sourceFileList, filesToImport);

            return remoteFileGroupToImport;
        }

        private IEnumerable<FileCollectionGroup> GetFilesByCheckType(RegexCodec regCod, bool hasDoneFile,
            string[] regexMaskGroupNames, List<SourceFile> sourceFileList, List<IFile> filesToImport)
        {
            var checkType = CurrentSource.AggregateProcessingSettings.FileLogCheckType ?? (regexMaskGroupNames.Length > 0 ? regCod.GetFileLogCheckType(regexMaskGroupNames) : regCod.GetFileLogCheckType()).ToString();

            _fileLogCheckType = UtilsText.ConvertToEnum<Constants.FileLogCheckType>(checkType);

            _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                PrefixJobGuid($"Checking file log based on check type: {_fileLogCheckType}.")));
            IEnumerable<FileCollectionGroup> remoteFileGroupToImport = null;
            switch (_fileLogCheckType)
            {
                case Constants.FileLogCheckType.All:
                    remoteFileGroupToImport = GetFilesByAllGroups(regCod, hasDoneFile, sourceFileList, filesToImport);
                    break;
                case Constants.FileLogCheckType.DatePlusEntityId:
                    remoteFileGroupToImport = GetFilesByDatePlusEntityId(regCod, hasDoneFile, sourceFileList, filesToImport);
                    break;
                case Constants.FileLogCheckType.DatePlusHour:
                    remoteFileGroupToImport = GetFilesByDatePlusHour(regCod, hasDoneFile, sourceFileList, filesToImport);
                    break;
                case Constants.FileLogCheckType.DateOnly:
                    remoteFileGroupToImport = GetFilesByDateOnly(regCod, hasDoneFile, sourceFileList, filesToImport);
                    break;
                case Constants.FileLogCheckType.YearMonthOnly:
                    remoteFileGroupToImport = GetFilesByYearMonthOnly(regCod, hasDoneFile, sourceFileList, filesToImport);
                    break;
                case Constants.FileLogCheckType.NameOnly:
                    remoteFileGroupToImport = GetFilesByName(regCod, hasDoneFile, sourceFileList, filesToImport);
                    break;
                case Constants.FileLogCheckType.None:
                    _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                        PrefixJobGuid($"No regex groups were found. Need at least the file date in the regex mask to compare against the file log.")));
                    return null;
            }

            return remoteFileGroupToImport;
        }

        private IEnumerable<FileCollectionGroup> GetFilesByYearMonthOnly(RegexCodec regCod, bool hasDoneFile, List<SourceFile> sourceFileList, List<IFile> filesToImport)
        {
            IEnumerable<FileCollectionGroup> remoteFileGroupToImport = null;
            //get processed files
            var processedFilesGrouped = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID)
                .Select(f => new
                {
                    f.FileDate
                });

            //get all files based on source file regex mask
            var remoteFilesGrouped = GetFileShellList(sourceFileList, filesToImport);

            if (hasDoneFile)
            {
                //get done files 
                var doneFilesByDate = GetFileShellList(regCod, filesToImport);

                //get done files that are not in file log
                var doneFilesToImport = doneFilesByDate.Where(s =>
                    processedFilesGrouped.All(p => p.FileDate != s.FileDate)).ToList();

                //get all files to import based on done files that are not in the file log
                remoteFileGroupToImport = remoteFilesGrouped.Where(x =>
                    doneFilesToImport.Any(d => d.FileDate == x.FileDate)).GroupBy(f =>
                    new
                    {
                        f.FileDate
                    }).Select(gf => new FileCollectionGroup()
                    {
                        FileDate = (DateTime)gf.Key.FileDate,
                        DistinctFileTypeCount = gf.Select(x => x.SourceFile).Distinct().Count(),
                        RemoteFiles = gf.Select(x => x.RemoteFile).ToList()
                    });
            }
            else
            {
                //Only import remote files not found in file log
                remoteFileGroupToImport = remoteFilesGrouped.Where(s =>
                    processedFilesGrouped.All(p => p.FileDate != s.FileDate)).GroupBy(f =>
                    new
                    {
                        f.FileDate
                    }).Select(gf => new FileCollectionGroup()
                    {
                        FileDate = (DateTime)gf.Key.FileDate,
                        DistinctFileTypeCount = gf.Select(x => x.SourceFile).Distinct().Count(),
                        RemoteFiles = gf.Select(x => x.RemoteFile).ToList()
                    });
            }

            return remoteFileGroupToImport;
        }

        private IEnumerable<FileCollectionGroup> GetFilesByDateOnly(RegexCodec regCod, bool hasDoneFile, List<SourceFile> sourceFileList, List<IFile> filesToImport)
        {
            IEnumerable<FileCollectionGroup> remoteFileGroupToImport = null;
            //get processed files
            var processedFilesGrouped = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID)
                .Select(f => new
                {
                    f.FileDate
                });

            //get all files based on source file regex mask
            var remoteFilesGrouped = GetFileShellList(sourceFileList, filesToImport);

            if (hasDoneFile)
            {
                //get done files 
                var doneFilesByDate = GetFileShellList(regCod, filesToImport);

                //get done files that are not in file log
                var doneFilesToImport = doneFilesByDate.Where(s =>
                    processedFilesGrouped.All(p => p.FileDate != s.FileDate)).ToList();

                //get all files to import based on done files that are not in the file log
                remoteFileGroupToImport = remoteFilesGrouped.Where(x =>
                    doneFilesToImport.Any(d => d.FileDate == x.FileDate)).GroupBy(f =>
                    new
                    {
                        f.FileDate
                    }).Select(gf => new FileCollectionGroup()
                    {
                        FileDate = gf.Key.FileDate.Value,
                        DistinctFileTypeCount = gf.Select(x => x.SourceFile).Distinct().Count(),
                        RemoteFiles = gf.Select(x => x.RemoteFile).ToList()
                    });
            }
            else
            {
                //Only import remote files not found in file log
                remoteFileGroupToImport = remoteFilesGrouped.Where(s =>
                    processedFilesGrouped.All(p => p.FileDate != s.FileDate)).GroupBy(f =>
                    new
                    {
                        f.FileDate
                    }).Select(gf => new FileCollectionGroup()
                    {
                        FileDate = gf.Key.FileDate.Value,
                        DistinctFileTypeCount = gf.Select(x => x.SourceFile).Distinct().Count(),
                        RemoteFiles = gf.Select(x => x.RemoteFile).ToList()
                    });
            }

            return remoteFileGroupToImport;
        }

        private IEnumerable<FileCollectionGroup> GetFilesByDatePlusHour(RegexCodec regCod, bool hasDoneFile, List<SourceFile> sourceFileList, List<IFile> filesToImport)
        {
            IEnumerable<FileCollectionGroup> remoteFileGroupToImport = null;
            //get processed files
            var processedFilesGrouped = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID)
                .Select(f => new
                {
                    f.FileDate,
                    f.FileDateHour
                });

            //get all files based on source file regex mask
            var remoteFilesGrouped = GetFileShellList(sourceFileList, filesToImport);

            if (hasDoneFile)
            {
                //get done files 
                var doneFilesByDate = GetFileShellList(regCod, filesToImport);

                //get done files that are not in file log
                var doneFilesToImport = doneFilesByDate.Where(s =>
                    !processedFilesGrouped.Any(p => p.FileDate == s.FileDate && p.FileDateHour == s.FileDateHour)).ToList();

                //get all files to import based on done files that are not in the file log
                remoteFileGroupToImport = remoteFilesGrouped.Where(x =>
                    doneFilesToImport.Any(d => d.FileDate == x.FileDate && d.FileDateHour == x.FileDateHour)).GroupBy(f =>
                    new
                    {
                        f.FileDate,
                        f.FileDateHour
                    }).Select(gf => new FileCollectionGroup()
                    {
                        FileDate = gf.Key.FileDate,
                        FileDateHour = gf.Key.FileDateHour,
                        DistinctFileTypeCount = gf.Select(x => x.SourceFile).Distinct().Count(),
                        RemoteFiles = gf.Select(x => x.RemoteFile).ToList()
                    });
            }
            else
            {
                //Only import remote files not found in file log
                remoteFileGroupToImport = remoteFilesGrouped.Where(s =>
                    !processedFilesGrouped.Any(p => p.FileDate == s.FileDate && p.FileDateHour == s.FileDateHour)).GroupBy(f =>
                    new
                    {
                        f.FileDate,
                        f.FileDateHour
                    }).Select(gf => new FileCollectionGroup()
                    {
                        FileDate = gf.Key.FileDate,
                        FileDateHour = gf.Key.FileDateHour,
                        DistinctFileTypeCount = gf.Select(x => x.SourceFile).Distinct().Count(),
                        RemoteFiles = gf.Select(x => x.RemoteFile).ToList()
                    });
            }

            return remoteFileGroupToImport;
        }

        private IEnumerable<FileCollectionGroup> GetFilesByAllGroups(RegexCodec regCod, bool hasDoneFile, List<SourceFile> sourceFileList, List<IFile> filesToImport)
        {
            IEnumerable<FileCollectionGroup> remoteFileGroupToImport = null;
            //get processed files
            var processedFilesGrouped = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID)
                .Select(f => new
                {
                    f.FileDate,
                    f.EntityID,
                    f.FileDateHour
                });

            //get all files based on source file regex mask
            var remoteFilesGrouped = GetFileShellList(sourceFileList, filesToImport);

            if (hasDoneFile)
            {
                //get done files 
                var doneFilesByDate = GetFileShellList(regCod, filesToImport);

                //get done files that are not in file log
                var doneFilesToImport = doneFilesByDate.Where(s =>
                    !processedFilesGrouped.Any(p => p.FileDate == s.FileDate && p.EntityID.Equals(s.EntityID, StringComparison.InvariantCultureIgnoreCase) && p.FileDateHour == s.FileDateHour)).ToList();

                //get all files to import based on done files that are not in the file log
                remoteFileGroupToImport = remoteFilesGrouped.Where(x =>
                    doneFilesToImport.Any(d => d.FileDate == x.FileDate && d.EntityID.Equals(x.EntityID, StringComparison.InvariantCultureIgnoreCase))).GroupBy(f =>
                    new
                    {
                        f.FileDate,
                        f.EntityID,
                        f.FileDateHour
                    }).Select(gf => new FileCollectionGroup()
                    {
                        FileDate = gf.Key.FileDate,
                        EntityID = gf.Key.EntityID,
                        FileDateHour = gf.Key.FileDateHour,
                        DistinctFileTypeCount = gf.Select(x => x.SourceFile).Distinct().Count(),
                        RemoteFiles = gf.Select(x => x.RemoteFile).ToList()
                    });
            }
            else
            {
                //Only import remote files not found in file log
                remoteFileGroupToImport = remoteFilesGrouped.Where(s =>
                    !processedFilesGrouped.Any(p => p.FileDate == s.FileDate && p.EntityID.Equals(s.EntityID, StringComparison.InvariantCultureIgnoreCase))).GroupBy(f =>
                    new
                    {
                        f.FileDate,
                        f.EntityID,
                        f.FileDateHour
                    }).Select(gf => new FileCollectionGroup()
                    {
                        FileDate = gf.Key.FileDate,
                        EntityID = gf.Key.EntityID,
                        FileDateHour = gf.Key.FileDateHour,
                        DistinctFileTypeCount = gf.Select(x => x.SourceFile).Distinct().Count(),
                        RemoteFiles = gf.Select(x => x.RemoteFile).ToList()
                    });
            }

            return remoteFileGroupToImport;
        }

        private IEnumerable<FileCollectionGroup> GetFilesByDatePlusEntityId(RegexCodec regCod, bool hasDoneFile, List<SourceFile> sourceFileList, List<IFile> filesToImport)
        {
            IEnumerable<FileCollectionGroup> remoteFileGroupToImport = null;
            //get processed files
            var processedFilesGrouped = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID)
                .Select(f => new
                {
                    f.FileDate,
                    f.EntityID
                });

            //get all files based on source file regex mask
            var remoteFilesGrouped = GetFileShellList(sourceFileList, filesToImport);

            if (hasDoneFile)
            {
                //get done files 
                var doneFilesByDate = GetFileShellList(regCod, filesToImport);

                //get done files that are not in file log
                var doneFilesToImport = doneFilesByDate.Where(s =>
                    !processedFilesGrouped.Any(p => p.FileDate == s.FileDate && p.EntityID.Equals(s.EntityID, StringComparison.InvariantCultureIgnoreCase))).ToList();

                //get all files to import based on done files that are not in the file log
                remoteFileGroupToImport = remoteFilesGrouped.Where(x =>
                    doneFilesToImport.Any(d => d.FileDate == x.FileDate && d.EntityID.Equals(x.EntityID, StringComparison.InvariantCultureIgnoreCase))).GroupBy(f =>
                    new
                    {
                        f.FileDate,
                        f.EntityID
                    }).Select(gf => new FileCollectionGroup()
                    {
                        FileDate = gf.Key.FileDate,
                        EntityID = gf.Key.EntityID,
                        DistinctFileTypeCount = gf.Select(x => x.SourceFile).Distinct().Count(),
                        RemoteFiles = gf.Select(x => x.RemoteFile).ToList()
                    });
            }
            else
            {
                //Only import remote files not found in file log
                remoteFileGroupToImport = remoteFilesGrouped.Where(s =>
                    !processedFilesGrouped.Any(p => p.FileDate == s.FileDate && p.EntityID.Equals(s.EntityID, StringComparison.InvariantCultureIgnoreCase))).GroupBy(f =>
                    new
                    {
                        f.FileDate,
                        f.EntityID
                    }).Select(gf => new FileCollectionGroup()
                    {
                        FileDate = gf.Key.FileDate,
                        EntityID = gf.Key.EntityID,
                        DistinctFileTypeCount = gf.Select(x => x.SourceFile).Distinct().Count(),
                        RemoteFiles = gf.Select(x => x.RemoteFile).ToList()
                    });
            }

            return remoteFileGroupToImport;
        }

        private List<FileCollectionGroup> GetFilesByName(RegexCodec regCod, bool hasDoneFile, List<SourceFile> sourceFileList, List<IFile> filesToImport)
        {
            //get processed files
            var processedFiles = Data.Services.JobService.GetAllFileLogs(CurrentIntegration.IntegrationID);

            //get done files 
            var doneFiles = filesToImport.Where(remote => regCod.FileNameRegex.IsMatch(remote.Name));

            //get done files that are not in file log
            var doneFilesToImport = doneFiles.Except(doneFiles.Where(s => processedFiles.Select(p => p.FileName).Contains(s.Name))).ToList();

            //get subset of import files that exclude done files
            var remoteFiles =
                filesToImport.Except(filesToImport.Where(i => doneFilesToImport.Select(d => d.Name).Contains(i.Name)));

            var remoteFileList = new List<FileCollectionGroup>();

            foreach (var doneFile in doneFilesToImport)
            {
                var doneFileName = doneFile.Name.Replace(doneFile.Extension, "");

                var remoteFilesGrouped = filesToImport.Where(x => x.Name.IndexOf(doneFileName) > -1);

                //if there is just 1 file (=done file) we dont add to the list
                if (remoteFilesGrouped.Count() <= 1) continue;

                var fileGroup = new FileCollectionGroup()
                {
                    HasDoneFile = true,
                    FileName = doneFile.Name,
                    RemoteFiles = remoteFilesGrouped.ToList()
                };

                remoteFileList.Add(fileGroup);
            }

            return remoteFileList;
        }

        private void ImportFileCollectionGroups(IEnumerable<FileCollectionGroup> remoteFileGroupToImport, List<SourceFile> sourceFileList)
        {
            foreach (var importingGroup in remoteFileGroupToImport)
            {
                var importingFileCollection = importingGroup.RemoteFiles;

                if (!importingGroup.HasDoneFile)
                {
                    /**
                     ** We need to account for duplicate file types. Check that we have all distinct file types.
                     ** Make sure we have a matching file type in [sourceFileList] for each individual file to be imported.
                     **/
                    var remoteFileFileTypeCount = importingGroup.DistinctFileTypeCount;
                    if (sourceFileList.Count != remoteFileFileTypeCount)
                    {
                        //if files older than offset (days) then we throw an exception to call attention to missing files
                        var offsetDays = (CurrentSource.DeliveryOffset == null) ? 1 : CurrentSource.DeliveryOffset.Value;
                        if ((DateTime.UtcNow - importingGroup.FileDate.Value.ToUniversalTime()).TotalDays > offsetDays)
                        {
                            var availableFiles = importingFileCollection.Select(f => f.Name).Aggregate((current, next) => current + "/" + next);
                            _logger.Log(Msg.Create(LogLevel.Warn, _logger.Name, PrefixJobGuid(
                                $"Not all file types are ready to be imported; Integration: {CurrentIntegration.IntegrationName}( {CurrentIntegration.IntegrationID} ); " +
                                $"FileDate: {importingGroup.FileDate}; Hour: {importingGroup.FileDateHour}; EntityID:{importingGroup.EntityID} - Total Source Files Required: {sourceFileList.Count}; " +
                                $"Current Files Available: {remoteFileFileTypeCount} | Available Files: {availableFiles}")));
                            warningCounter++;
                        }
                        continue;
                    }
                }

                CopyFileCollection(importingFileCollection, importingGroup.HasDoneFile, importingGroup.FileName, sourceFileList);
            }
        }

        private void CopyFileCollection(List<IFile> importingFileCollection, bool importingGroupHasDoneFile, string importingGroupFileName, List<SourceFile> sourceFileList)
        {
            //prepare file to copy
            var importQueueFileCollection = new List<Data.Model.Core.Queue>();
            var importFile = new Data.Model.Core.Queue()
            {
                FileGUID = Guid.NewGuid(),
                IntegrationID = CurrentIntegration.IntegrationID,
                SourceID = CurrentSource.SourceID,
                JobLogID = JobLogger.JobLog.JobLogID,
                Step = JED.Step.ToString(),
                SourceFileName = (sourceFileList.Select(x => x.SourceFileName).Distinct().Count() > 1) ? $"{CurrentSource.SourceName}Reports" : sourceFileList.First().SourceFileName,
                DeliveryFileDate = importingFileCollection.Max(file => file.LastWriteTimeUtc)
            };

            //import pending file
            foreach (IFile incomingFile in importingFileCollection)
            {
                try
                {
                    //check that incomingFile has matching source file
                    var currentSourceFile = sourceFileList.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(incomingFile.Name));
                    if (currentSourceFile == null)
                    {
                        _logger.Log(Msg.Create(LogLevel.Info, _logger.Name,
                            PrefixJobGuid($"Filename: {incomingFile.Name} skipped because no matching source file found")));
                        continue;
                    }

                    //can we extract datetime from the filename?
                    var checkFileDate = _fileLogCheckType != Constants.FileLogCheckType.YearMonthOnly;

                    if (currentSourceFile.FileRegexCodec.TryParse(incomingFile.Name, checkFileDate))
                    {
                        _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                            PrefixJobGuid($"incomingFile.Name: {incomingFile.Name}. sf: {JsonConvert.SerializeObject(currentSourceFile)}")));
                        importFile.FileDate = currentSourceFile.FileRegexCodec.FileNameDate ?? incomingFile.LastWriteTimeUtc;
                        importFile.FileDateHour = currentSourceFile.FileRegexCodec.FileNameHour;
                        importFile.EntityID = currentSourceFile.FileRegexCodec.EntityId;
                    }
                    else
                    {
                        importFile.FileDate = incomingFile.LastWriteTimeUtc;
                    }

                    //basebucket/raw/source/entityid/date 
                    string[] paths = new string[] { importFile.EntityID?.ToLower() ?? string.Empty, GetDatedPartition(importFile.FileDate), incomingFile.Name };
                    Uri destUri = RemoteUri.CombineUri(_baseDestUri, paths);
                    _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                        PrefixJobGuid(
                            $"destUri: {JsonConvert.SerializeObject(destUri)}. paths: {JsonConvert.SerializeObject(paths)}")));
                    IFile destFile = new S3File(destUri, GreenhouseS3Creds);

                    base.UploadToS3(incomingFile, (S3File)destFile, paths);

                    filesIn++;
                    bytesIn += incomingFile.Length;

                    if (!currentSourceFile.IsDoneFile)
                    {
                        //Add the completed transfer file collection to the transfer log.
                        importQueueFileCollection.Add(new Data.Model.Core.Queue
                        {
                            SourceFileName = currentSourceFile.SourceFileName,
                            FileName = incomingFile.Name,
                            FileSize = incomingFile.Length
                        });

                        if (CurrentSource.AggregateProcessingSettings.PrefixFileGuid)
                        {
                            /**
                             *StageFileCollection stages raw files with fileguid if flag enabled "prefixFileGuid"
                             *Created for Adelphic and sources that need to process file collections that are stored in the same s3 partition
                             *Example: File_9922_147322_2019-11-03.tsv.gz and File_9922_223741_2019-11-03.tsv.gz are stored in the same s3 partition, ie "adelphic-aggregate/entityid=9922/date=2019-11-03/"
                             *Adding a fileguid to each file will allow the processing job to process each file separately, ie fileguid1_File_9922_147322_2019-11-03.tsv.gz and fileguid2_File_9922_223741_2019-11-03.tsv.gz
                             *Because a fileguid is necessary for processing, we stage the files in order to keep the files in raw as close in resemblance to what we received in the intake bucket
                             */
                            StageFileCollection(incomingFile, importFile);
                        }
                    }
                }
                catch (Exception exc)
                {
                    _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid(
                        $"Import failed on file {incomingFile.Uri} - Size: {incomingFile.Length} -> Exception: {exc.Message} - STACK {exc.StackTrace}"), exc));
                    exceptionCounter++;
                    return;
                }
            }

            AddFileToQueue(importFile, importQueueFileCollection, importingGroupFileName);
        }

        private void StageFileCollection(IFile incomingFile, Data.Model.Core.Queue importFile)
        {
            string[] paths = new string[] { importFile.EntityID?.ToLower() ?? string.Empty, GetDatedPartition(importFile.FileDate), $"{importFile.FileGUID}_{incomingFile.Name}" };

            Uri stageUri = new Uri(this._baseDestUri.ToString()
                .Replace(Constants.ProcessingStage.RAW.ToString().ToLower(),
                    Constants.ProcessingStage.STAGE.ToString().ToLower()));

            Uri destUri = RemoteUri.CombineUri(stageUri, paths);

            _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                PrefixJobGuid(
                    $"Staging files with fileGUID prefix - destUri: {JsonConvert.SerializeObject(destUri)}. paths: {JsonConvert.SerializeObject(paths)}")));
            IFile destFile = new S3File(destUri, GreenhouseS3Creds);

            base.UploadToS3(incomingFile, (S3File)destFile, paths);
        }

        private void AddFileToQueue(Data.Model.Core.Queue importFile, List<Data.Model.Core.Queue> importQueueFileCollection, string doneFileName)
        {
            try
            {
                //store files to be imported csv of FileType:FilePath.
                var files = importQueueFileCollection.Select(x => new FileCollectionItem()
                {
                    FilePath = x.FileName,
                    SourceFileName = x.SourceFileName,
                    FileSize = x.FileSize
                });

                var filesJson = JsonConvert.SerializeObject(files);

                if (string.IsNullOrEmpty(doneFileName))
                {
                    var fileNameParts = new List<string>(new string[] { $"{CurrentSource.SourceName}Reports", $"{importFile.FileDate:yyyyMMdd}" });
                    if (!string.IsNullOrEmpty(importFile.EntityID))
                    {
                        fileNameParts.Add(importFile.EntityID);
                    }
                    importFile.FileName = (importQueueFileCollection.Count > 1)
                        ? fileNameParts.Select(x => x).Aggregate((current, next) => current + '_' + next) : importQueueFileCollection.First().FileName;
                    if (importQueueFileCollection.Count > 1)
                        importFile.FileCollectionJSON = filesJson;
                }
                else
                {
                    importFile.FileName = doneFileName;
                    importFile.FileCollectionJSON = filesJson;
                }

                importFile.Status = Constants.JobStatus.Complete.ToString();
                importFile.StatusId = (int)Constants.JobStatus.Complete;
                importFile.FileSize = bytesIn;
                Data.Services.JobService.Add(importFile);

                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                    PrefixJobGuid($"Successfully queued {CurrentSource.SourceName} report files {filesJson}.")));
            }
            catch (Exception exc)
            {
                _logger.Log(Msg.Create(LogLevel.Error, _logger.Name, base.PrefixJobGuid(
                    $"Error queuing {CurrentSource.SourceName} files {JsonConvert.SerializeObject(importQueueFileCollection)} -> Exception: {exc.Message} - STACK {exc.StackTrace}"), exc));
                exceptionCounter++;
            }
        }

        private bool CheckRemoteFilesRegex(List<SourceFile> sourceFileList)
        {
            var remoteFilesNoRegexMatch = _importFiles.Where(x =>
                sourceFileList.SingleOrDefault(s => s.FileRegexCodec.FileNameRegex.IsMatch(x.Name)) == null).ToList();
            var fileListMissingRegex = string.Join(",", remoteFilesNoRegexMatch);
            if (remoteFilesNoRegexMatch.Count != 0)
            {
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                    PrefixJobGuid(
                        $"There are remote files that do not have a matching source file; total count : {remoteFilesNoRegexMatch.Count} file list: {fileListMissingRegex}")));

                return true;
            }
            else
            {
                _logger.Log(Msg.Create(LogLevel.Debug, _logger.Name,
                    PrefixJobGuid(
                        $"Remote Files Regex Check Complete - there are remote files have a matching source file")));

                return false;
            }
        }

        private sealed class FileCollectionGroup
        {
            public int DistinctFileTypeCount { get; set; }
            public List<IFile> RemoteFiles { get; set; }
            public bool HasDoneFile { get; set; }
            public DateTime? FileDate { get; set; }
            public int? FileDateHour { get; set; }
            public string EntityID { get; set; }
            public string FileName { get; set; }
        }

        public void PostExecute()
        {
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                _rac?.Dispose();
            }
        }

        ~AggregateImportJob()
        {
            Dispose(false);
        }

        public string GetJobCacheKey()
        {
            return DefaultJobCacheKey;
        }

        private sealed class FileShell
        {
            public IFile RemoteFile { get; set; }
            public string EntityID { get; set; }
            public DateTime? FileDate { get; set; }
            public int? FileDateHour { get; set; }
            public SourceFile SourceFile { get; set; }
        }
    }
}
