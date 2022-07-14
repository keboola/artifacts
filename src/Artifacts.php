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
    private StorageClient $storageClient;
    private Filesystem $filesystem;
    private LoggerInterface $logger;
    private string $branchId;
    private string $componentId;
    private ?string $configId;
    private string $jobId;
    private ?string $orchestrationId;

    public function __construct(
        StorageClient $storageClient,
        LoggerInterface $logger,
        Temp $temp,
        string $branchId,
        string $componentId,
        ?string $configId,
        string $jobId,
        ?string $orchestrationId = null
    ) {
        $this->storageClient = $storageClient;
        $this->logger = $logger;
        $this->filesystem = new Filesystem($temp);
        $this->branchId = $branchId;
        $this->componentId = $componentId;
        $this->configId = $configId;
        $this->jobId = $jobId;
        $this->orchestrationId = $orchestrationId;
    }

    public function getFilesystem(): Filesystem
    {
        return $this->filesystem;
    }

    public function upload(): array
    {
        if (!$this->checkConfigId()) {
            return [];
        }

        $uploaded = [];
        $uploaded[] = $this->uploadArtifact($this->filesystem->getUploadCurrentDir());

        if ($this->orchestrationId) {
            $uploaded[] = $this->uploadArtifact($this->filesystem->getUploadSharedDir(), [
                'shared',
                sprintf('orchestrationId-%s', $this->orchestrationId),
            ]);
        }

        return array_filter($uploaded);
    }

    public function download(array $configuration): array
    {
        if (!$this->checkConfigId()) {
            return [];
        }

        if (!empty($configuration['artifacts']['runs']['enabled'])) {
            $artifactsConfiguration = $configuration['artifacts'];
            return $this->downloadRuns(
                $artifactsConfiguration['runs']['filter']['limit'] ?? null,
                $artifactsConfiguration['runs']['filter']['date_since'] ?? null,
            );
        }

        if (!empty($configuration['artifacts']['shared']['enabled'])) {
            return $this->downloadShared();
        }

        return [];
    }

    private function downloadRuns(
        ?int $limit = null,
        ?string $dateSince = null
    ): array {
        if (is_null($this->configId)) {
            $this->logger->warning('Skipping download of artifacts, configuration Id is not set');
            return [];
        }

        $query = sprintf(
            'tags:(artifact AND branchId-%s AND componentId-%s AND configId-%s)',
            $this->branchId,
            $this->componentId,
            $this->configId
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
                $dstPath = $this->filesystem->getDownloadRunsJobDir($jobId);
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

    private function downloadShared(): array
    {
        if (!$this->orchestrationId) {
            return [];
        }

        $tagsQuery = sprintf(
            'artifact AND shared AND branchId-%s AND componentId-%s AND configId-%s AND orchestrationId-%s',
            $this->branchId,
            $this->componentId,
            $this->configId,
            $this->orchestrationId
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

    private function uploadArtifact(string $directory, array $addTags = []): ?array
    {
        $finder = new Finder();
        $count = $finder->in($directory)->count();
        if ($count === 0) {
            return null;
        }

        try {
            $this->filesystem->archiveDir($directory, $this->filesystem->getArchivePath());

            $options = new FileUploadOptions();
            $options->setTags(array_merge([
                'artifact',
                'branchId-' . $this->branchId,
                'componentId-' . $this->componentId,
                'configId-' . $this->configId,
                'jobId-' . $this->jobId,
            ], $addTags));

            $fileId = $this->storageClient->uploadFile($this->filesystem->getArchivePath(), $options);
            $this->logger->info(sprintf('Uploaded artifact for job "%s" to file "%s"', $this->jobId, $fileId));
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

    private function checkConfigId(): bool
    {
        if (is_null($this->configId)) {
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
