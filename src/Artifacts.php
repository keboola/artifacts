<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

use DateTime;
use JetBrains\PhpStorm\ArrayShape;
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

    public function __construct(
        StorageClient $storageClient,
        LoggerInterface $logger,
        Temp $temp,
        string $branchId,
        string $componentId,
        ?string $configId,
        string $jobId
    ) {
        $this->storageClient = $storageClient;
        $this->logger = $logger;
        $this->filesystem = new Filesystem($temp);
        $this->branchId = $branchId;
        $this->componentId = $componentId;
        $this->configId = $configId;
        $this->jobId = $jobId;
    }

    public function getFilesystem(): Filesystem
    {
        return $this->filesystem;
    }

    public function uploadCurrent(): ?array
    {
        if (is_null($this->configId)) {
            $this->logger->warning('Skipping upload of artifacts, configuration Id is not set');
            return null;
        }

        $currentDir = $this->filesystem->getCurrentDir();
        $finder = new Finder();
        $count = $finder->in($currentDir)->count();
        if ($count === 0) {
            return null;
        }

        try {
            $this->filesystem->archiveDir($currentDir, $this->filesystem->getArchivePath());

            $options = new FileUploadOptions();
            $options->setTags([
                'artifact',
                'branchId-' . $this->branchId,
                'componentId-' . $this->componentId,
                'configId-' . $this->configId,
                'jobId-' . $this->jobId,
            ]);

            $fileId = $this->storageClient->uploadFile($this->filesystem->getArchivePath(), $options);
            $this->logger->info(sprintf('Uploaded artifact for job "%s" to file "%s"', $this->jobId, $fileId));
            return $this->fileToResult($fileId);
        } catch (ProcessFailedException | ClientException $e) {
            throw new ArtifactsException(sprintf('Error uploading file: %s', $e->getMessage()), 0, $e);
        }
    }

    public function downloadLatestRuns(
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
                $dstPath = $this->filesystem->getJobRunDir($jobId);
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

    private function fileToResult(int $fileId): array
    {
        return [
            'storageFileId' => $fileId,
        ];
    }
}
