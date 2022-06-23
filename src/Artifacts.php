<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

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
    private StorageClient $storageClient;
    private Filesystem $filesystem;
    private LoggerInterface $logger;
    private string $branchId;
    private string $componentId;
    private string $configId;
    private string $jobId;

    public function __construct(
        StorageClient $storageClient,
        LoggerInterface $logger,
        Temp $temp,
        string $branchId,
        string $componentId,
        string $configId,
        string $jobId
    ) {
        $this->storageClient = $storageClient;
        // todo setRunId?
//        $storageClient->setRunId($runId);

        $this->logger = $logger;
        $this->filesystem = new Filesystem($temp);
        $this->branchId = $branchId;
        $this->componentId = $componentId;
        $this->configId = $configId;
        $this->jobId = $jobId;
    }

    public function uploadCurrent(): ?int
    {
        $currentDir = $this->filesystem->getRunsCurrentDir();

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
            return $fileId;
        } catch (ProcessFailedException | ClientException $e) {
            throw new ArtifactsException(sprintf('Error uploading file: %s', $e->getMessage()), 0, $e);
        }
    }

    public function downloadLatestRuns(
        ?int $limit = null
    ): array {
        $options = new ListFilesOptions();
        $options->setQuery(sprintf(
            'tags:(artifact AND componentId-%s AND configId-%s)',
            $this->componentId,
            $this->configId
        ));

        if ($limit) {
            $options->setLimit($limit);
        }

        return $this->storageClient->listFiles($options);
    }
}
