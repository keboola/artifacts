<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

use JetBrains\PhpStorm\Pure;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
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

    private StorageClient $storageClient;
    private Filesystem $filesystem;
    private LoggerInterface $logger;

    public const ZIP_DEFAULT = true;

    public function __construct(
        StorageClient $storageClient,
        LoggerInterface $logger,
        Temp $temp
    ) {
        $this->storageClient = $storageClient;
        $this->logger = $logger;
        $this->filesystem = new Filesystem($temp);
    }

    public function getFilesystem(): Filesystem
    {
        return $this->filesystem;
    }

    /** @return Result[] */
    public function upload(Tags $tags, array $configuration = []): array
    {
        if (!$this->checkConfigId($tags)) {
            return [];
        }

        $results = $this->uploadArtifacts(
            $this->filesystem->getUploadCurrentDir(),
            $tags,
            $configuration
        );

        if ($tags->getOrchestrationId()) {
            array_push($results, ...$this->uploadArtifacts(
                $this->filesystem->getUploadSharedDir(),
                $tags->setIsShared(true),
                $configuration
            ));
        }

        return $results;
    }

    /** @return Result[] */
    public function download(Tags $tags, array $configuration): array
    {
        if (!$this->checkConfigId($tags)) {
            return [];
        }

        $isArchive = $configuration['artifacts']['options']['zip'] ?? self::ZIP_DEFAULT;
        if (!empty($configuration['artifacts']['runs']['enabled'])) {
            $artifactsRunsConfiguration = $configuration['artifacts']['runs'];
            return $this->downloadRuns(
                Tags::mergeWithConfiguration($tags, $configuration),
                $artifactsRunsConfiguration['filter']['limit'] ?? null,
                $artifactsRunsConfiguration['filter']['date_since'] ?? null,
                self::DOWNLOAD_TYPE_RUNS,
                $isArchive,
            );
        }

        if (!empty($configuration['artifacts']['shared']['enabled'])) {
            return $this->downloadShared($tags->setIsShared(true), $isArchive);
        }

        return [];
    }

    /** @return Result[] */
    private function downloadRuns(
        Tags $tags,
        ?int $limit,
        ?string $dateSince,
        string $type,
        bool $isArchive
    ): array {
        if (is_null($tags->getConfigId())) {
            $this->logger->warning('Skipping download of artifacts, configuration Id is not set');
            return [];
        }

        $query = $tags->toDownloadRunsQuery($dateSince);

        if ($limit === null || $limit > self::DOWNLOAD_FILES_MAX_LIMIT) {
            $limit = self::DOWNLOAD_FILES_MAX_LIMIT;
        }

        $files = $this->storageClient->listFiles(
            (new ListFilesOptions())
                ->setQuery($query)
                ->setLimit($limit)
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
                    $file['id'],
                    $e->getMessage()
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
    private function downloadShared(Tags $tags, bool $isArchive): array
    {
        if (!$tags->getOrchestrationId()) {
            return [];
        }

        $files = $this->storageClient->listFiles(
            (new ListFilesOptions())
                ->setQuery($tags->toDownloadSharedQuery())
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
                    $file['id'],
                    $e->getMessage()
                ));
            }
        }

        return $result;
    }

    /** @return Result[] */
    private function uploadArtifacts(string $directory, Tags $tags, array $configuration): array
    {
        $finder = new Finder();
        $count = $finder->in($directory)->count();
        if ($count === 0) {
            return [];
        }

        try {
            $results = [];
            if ($configuration['artifacts']['options']['zip'] ?? self::ZIP_DEFAULT) {
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

                $fileId = $this->storageClient->uploadFile($file->getPathname(), $fileUploadOptions);
                $this->logger->info(sprintf(
                    'Uploaded artifact for job "%s" to file "%s"',
                    $tags->getJobId(),
                    $fileId
                ));
                $results[] = new Result($fileId, $tags->getIsShared());
            }
            return $results;
        } catch (ProcessFailedException | ClientException $e) {
            throw new ArtifactsException(sprintf('Error uploading file: %s', $e->getMessage()), 0, $e);
        }
    }

    private function downloadFile(array $file, string $dstDir, bool $isArchive): Result
    {
        if ($isArchive) {
            $tmpPath = $this->filesystem->getTmpDir() . '/' . $file['id'];
            $this->storageClient->downloadFile($file['id'], $tmpPath);
            $this->filesystem->extractArchive($tmpPath, $dstDir);
        } else {
            $this->filesystem->mkdir($dstDir);
            $this->storageClient->downloadFile($file['id'], sprintf('%s/%s', $dstDir, $file['name']));
        }

        return new Result($file['id']);
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
