<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

use Keboola\Artifacts\Tags\TagsToQueryRunsProcessor;
use Keboola\Artifacts\Tags\TagsToQuerySharedProcessor;
use Keboola\JobQueue\JobConfiguration\JobDefinition\Configuration\Artifacts\Artifacts as ArtifactsConfiguration;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
use SplFileInfo;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Exception\ProcessFailedException;

class Artifacts
{
    public const DOWNLOAD_FILES_MAX_LIMIT = 50;
    public const DOWNLOAD_TYPE_RUNS = 'runs';
    public const DOWNLOAD_TYPE_SHARED = 'shared';
    public const DOWNLOAD_TYPE_CUSTOM = 'custom';

    private ClientWrapper $clientWrapper;
    private Filesystem $filesystem;
    private LoggerInterface $logger;

    public const ZIP_DEFAULT = true;

    public function __construct(
        ClientWrapper $clientWrapper,
        LoggerInterface $logger,
        Temp $temp,
    ) {
        $this->clientWrapper = $clientWrapper;
        $this->logger = $logger;
        $this->filesystem = new Filesystem($temp);
    }

    public function getFilesystem(): Filesystem
    {
        return $this->filesystem;
    }

    /** @return Result[] */
    public function upload(Tags $tags, ArtifactsConfiguration $artifactsConfig): array
    {
        if (!$this->checkConfigId($tags)) {
            return [];
        }

        $results = $this->uploadArtifacts(
            $this->filesystem->getUploadCurrentDir(),
            $tags,
            $artifactsConfig,
        );

        if ($tags->getOrchestrationId()) {
            array_push($results, ...$this->uploadArtifacts(
                $this->filesystem->getUploadSharedDir(),
                $tags->setIsShared(true),
                $artifactsConfig,
            ));
        }

        return $results;
    }

    /** @return Result[] */
    public function download(Tags $tags, ArtifactsConfiguration $artifactsConfig): array
    {
        if (!$this->checkConfigId($tags)) {
            return [];
        }

        $isArchive = $artifactsConfig->options->zip ?? self::ZIP_DEFAULT;

        if (!empty($artifactsConfig->runs->enabled)) {
            $filter = $artifactsConfig->runs->filter;
            return $this->downloadRuns(
                Tags::mergeWithConfiguration($tags, $filter),
                $filter->limit ?? null,
                $filter->dateSince ?? null,
                self::DOWNLOAD_TYPE_RUNS,
                $isArchive,
            );
        }

        if (!empty($artifactsConfig->custom->enabled)) {
            $filter = $artifactsConfig->custom->filter;
            return $this->downloadRuns(
                Tags::mergeWithConfiguration($tags, $filter),
                $filter->limit ?? null,
                $filter->dateSince ?? null,
                self::DOWNLOAD_TYPE_CUSTOM,
                $isArchive,
            );
        }

        if (!empty($artifactsConfig->shared->enabled)) {
            $filter = $artifactsConfig->custom->filter;
            return $this->downloadShared(
                $tags->setIsShared(true),
                $filter->limit ?? null,
                $isArchive,
            );
        }

        return [];
    }

    /** @return Result[] */
    private function downloadRuns(
        Tags $tags,
        ?int $limit,
        ?string $dateSince,
        string $type,
        bool $isArchive,
    ): array {
        if (is_null($tags->getConfigId())) {
            $this->logger->warning('Skipping download of artifacts, configuration Id is not set');
            return [];
        }

        if ($limit === null || $limit > self::DOWNLOAD_FILES_MAX_LIMIT) {
            $limit = self::DOWNLOAD_FILES_MAX_LIMIT;
        }

        $files = StorageFileHelper::listFiles(
            $this->clientWrapper,
            $tags,
            $limit,
            new TagsToQueryRunsProcessor($dateSince),
        );

        $result = [];
        foreach ($files as $file) {
            try {
                $jobId = StorageFileHelper::getJobIdFromFileTag($file);
                $dstPath = $this->resolveDownloadPath($jobId, $type);
                $result[] = $this->downloadFile($file, $dstPath, $isArchive);
            } catch (ArtifactsException $e) {
                $this->logger->warning(sprintf(
                    'Error downloading run artifact file id "%s": %s',
                    $file->id,
                    $e->getMessage(),
                ));
            }
        }

        return $result;
    }

    private function resolveDownloadPath(string $jobId, string $type): string
    {
        if ($type === self::DOWNLOAD_TYPE_CUSTOM) {
            return $this->filesystem->getDownloadCustomJobsDir($jobId);
        }
        return $this->filesystem->getDownloadRunsJobDir($jobId);
    }

    /** @return Result[] */
    private function downloadShared(Tags $tags, ?int $limit, bool $isArchive): array
    {
        if (!$tags->getOrchestrationId()) {
            return [];
        }

        if ($limit === null || $limit > self::DOWNLOAD_FILES_MAX_LIMIT) {
            $limit = self::DOWNLOAD_FILES_MAX_LIMIT;
        }

        $files = StorageFileHelper::listFiles(
            $this->clientWrapper,
            $tags,
            $limit,
            new TagsToQuerySharedProcessor(),
        );

        $result = [];
        foreach ($files as $file) {
            try {
                $jobId = StorageFileHelper::getJobIdFromFileTag($file);
                $dstPath = $this->filesystem->getDownloadSharedJobsDir($jobId);
                $result[] = $this->downloadFile($file, $dstPath, $isArchive);
            } catch (ArtifactsException $e) {
                $this->logger->warning(sprintf(
                    'Error downloading artifact file id "%s": %s',
                    $file->id,
                    $e->getMessage(),
                ));
            }
        }

        return $result;
    }

    /** @return Result[] */
    private function uploadArtifacts(string $directory, Tags $tags, ArtifactsConfiguration $artifactsConfig): array
    {
        $finder = new Finder();
        $count = $finder->in($directory)->count();
        if ($count === 0) {
            return [];
        }

        try {
            $results = [];
            if ($artifactsConfig->options->zip ?? self::ZIP_DEFAULT) {
                $this->filesystem->archiveDir($directory, $this->filesystem->getArchivePath());
                $this->filesystem->checkFileSize($this->filesystem->getArchivePath());
                $files[] = new SplFileInfo($this->filesystem->getArchivePath());
            } else {
                $finder = new Finder();
                $files = $finder
                    ->files()
                    ->in($directory)
                    ->sortByName();
            }

            foreach ($files as $file) {
                $fileUploadOptions = new FileUploadOptions();
                $fileUploadOptions->setTags($tags->toUploadArray());

                $fileId = $this->clientWrapper->getTableAndFileStorageClient()->uploadFile(
                    $file->getPathname(),
                    $fileUploadOptions,
                );
                $this->logger->info(sprintf(
                    'Uploaded artifact for job "%s" to file "%s"',
                    $tags->getJobId(),
                    $fileId,
                ));
                $results[] = new Result($fileId, $tags->getIsShared());
            }
            return $results;
        } catch (ProcessFailedException | ClientException $e) {
            throw new ArtifactsException(sprintf('Error uploading file: %s', $e->getMessage()), 0, $e);
        }
    }

    private function downloadFile(File $file, string $dstDir, bool $isArchive): Result
    {
        if ($isArchive) {
            $tmpPath = $this->filesystem->getTmpDir() . '/' . $file->id;
            $this->clientWrapper->getClientForBranch($file->sourceBranchId)->downloadFile($file->id, $tmpPath);
            $this->filesystem->extractArchive($tmpPath, $dstDir);
        } else {
            $this->filesystem->mkdir($dstDir);
            $this->clientWrapper->getClientForBranch($file->sourceBranchId)->downloadFile(
                $file->id,
                sprintf('%s/%s', $dstDir, $file->name),
            );
        }

        return new Result((int) $file->id);
    }

    private function checkConfigId(Tags $tags): bool
    {
        if (is_null($tags->getConfigId())) {
            $this->logger->warning('Ignoring artifacts, configuration Id is not set');
            return false;
        }

        return true;
    }
}
