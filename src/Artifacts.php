<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

use DateTime;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\Temp\Temp;
use Psr\Log\LoggerInterface;
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

    public function __construct(
        StorageClient $storageClient,
        LoggerInterface $logger,
        Temp $temp,
    ) {
        $this->storageClient = $storageClient;
        $this->logger = $logger;
        $this->filesystem = new Filesystem($temp);
    }

    public function getFilesystem(): Filesystem
    {
        return $this->filesystem;
    }

    public function upload(Tags $tags): array
    {
        if (!$this->checkConfigId($tags)) {
            return [];
        }

        $uploaded = [];
        $uploaded[] = $this->uploadArtifact(
            $this->filesystem->getUploadCurrentDir(),
            $tags
        );

        if ($tags->getOrchestrationId()) {
            $uploaded[] = $this->uploadArtifact(
                $this->filesystem->getUploadSharedDir(),
                $tags
            );
        }

        return array_filter($uploaded);
    }

    public function download(Tags $tags, array $configuration): array
    {
        if (!$this->checkConfigId($tags)) {
            return [];
        }

        if (!empty($configuration['artifacts']['runs']['enabled'])) {
            $artifactsRunsConfiguration = $configuration['artifacts']['runs'];
            return $this->downloadRuns(
                $tags,
                $artifactsRunsConfiguration['filter']['limit'] ?? null,
                $artifactsRunsConfiguration['filter']['date_since'] ?? null,
            );
        }

        if (!empty($configuration['artifacts']['custom']['enabled'])) {
            $artifactsCustomConfiguration = $configuration['artifacts']['custom'];
            return $this->downloadRuns(
                Tags::fromConfiguration($configuration),
                $artifactsCustomConfiguration['filter']['limit'] ?? null,
                $artifactsCustomConfiguration['filter']['date_since'] ?? null,
                self::DOWNLOAD_TYPE_CUSTOM
            );
        }

        if (!empty($configuration['artifacts']['shared']['enabled'])) {
            return $this->downloadShared($tags);
        }

        return [];
    }

    private function downloadRuns(
        Tags $tags,
        ?int $limit = null,
        ?string $dateSince = null,
        string $type = 'runs'
    ): array {
        if (is_null($tags->getConfigId())) {
            $this->logger->warning('Skipping download of artifacts, configuration Id is not set');
            return [];
        }

        $query = sprintf(
            'tags:(artifact AND branchId-%s AND componentId-%s AND configId-%s NOT shared)',
            $tags->getBranchId(),
            $tags->getComponentId(),
            $tags->getConfigId()
        );
        if ($dateSince) {
            $dateUTC = (new DateTime($dateSince))->format('Y-m-d');
            $query .= ' AND created:>' . $dateUTC;
        }
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
                $tmpPath = $this->filesystem->getTmpDir() . '/' . $file['id'];
                $this->storageClient->downloadFile($file['id'], $tmpPath);
                $dstPath = $this->resolveDownloadPath($jobId, $type);
                $this->filesystem->extractArchive($tmpPath, $dstPath);
                $result[] = $this->fileToResult($file['id']);
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

    private function downloadShared(Tags $tags): array
    {
        if (!$tags->getOrchestrationId()) {
            return [];
        }

        $tagsQuery = sprintf(
            'artifact AND shared AND branchId-%s AND orchestrationId-%s',
            $tags->getBranchId(),
            $tags->getOrchestrationId()
        );
        $query = sprintf('tags:(%s)', $tagsQuery);

        $files = $this->storageClient->listFiles(
            (new ListFilesOptions())
                ->setQuery($query)
        );

        $result = [];
        foreach ($files as $file) {
            try {
                $jobId = StorageFileHelper::getJobIdFromFileTag($file);
                $dstPath = $this->filesystem->getDownloadSharedJobsDir($jobId);
                $result[] = $this->downloadArtifact($file, $dstPath);
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

    private function uploadArtifact(string $directory, Tags $tags): ?array
    {
        $finder = new Finder();
        $count = $finder->in($directory)->count();
        if ($count === 0) {
            return null;
        }

        try {
            $this->filesystem->archiveDir($directory, $this->filesystem->getArchivePath());

            $options = new FileUploadOptions();
            $options->setTags($tags->toArray());

            $fileId = $this->storageClient->uploadFile($this->filesystem->getArchivePath(), $options);
            $this->logger->info(sprintf(
                'Uploaded artifact for job "%s" to file "%s"',
                $tags->getJobId(),
                $fileId
            ));
            return $this->fileToResult($fileId);
        } catch (ProcessFailedException | ClientException $e) {
            throw new ArtifactsException(sprintf('Error uploading file: %s', $e->getMessage()), 0, $e);
        }
    }

    private function downloadArtifact(array $file, string $dstPath): array
    {
        $tmpPath = $this->filesystem->getTmpDir() . '/' . $file['id'];
        $this->storageClient->downloadFile($file['id'], $tmpPath);
        $this->filesystem->extractArchive($tmpPath, $dstPath);

        return $this->fileToResult($file['id']);
    }

    private function checkConfigId(Tags $tags): bool
    {
        if (is_null($tags->getConfigId())) {
            $this->logger->warning('Ignoring artifacts, configuration Id is not set');
            return false;
        }

        return true;
    }

    private function fileToResult(int $fileId): array
    {
        return [
            'storageFileId' => $fileId,
        ];
    }
}
