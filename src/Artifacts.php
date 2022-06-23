<?php

declare(strict_types = 1);

namespace Keboola\Artifacts;

use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Options\ListFilesOptions;
use Psr\Log\LoggerInterface;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Exception\ProcessFailedException;

class Artifacts
{
    private StorageClient $storageClient;
    private Filesystem $filesystem;
    private LoggerInterface $logger;
    private string $componentId;
    private string $configId;
    private string $jobId;

    private const ARTIFACTS_ARCHIVE_PATH = '/tmp/artefacts.tar.gz';

    public function __construct(
        StorageClient $storageClient,
        LoggerInterface $logger,
        string $dataDir,
        string $componentId,
        string $configId,
        string $jobId
    ) {
        $this->storageClient = $storageClient;
        $this->logger = $logger;
        $this->filesystem = new Filesystem($dataDir);
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
            $this->filesystem->archiveDir($currentDir, self::ARTIFACTS_ARCHIVE_PATH);

            $options = new FileUploadOptions();
            $options->setTags([
                'artifact',
                'componentId-' . $this->componentId,
                'configId-' . $this->configId,
                'jobId-' . $this->jobId,
            ]);

            $fileId = $this->storageClient->uploadFile(self::ARTIFACTS_ARCHIVE_PATH, $options);
            $this->logger->info(sprintf('Uploaded artifact for job "%s" to file "%s"', $this->jobId, $fileId));
            return $fileId;
        } catch (ProcessFailedException | ClientException $e) {
            throw new ArtifactsException(sprintf('Error uploading file: %s', $e->getMessage()), 0, $e);
        }
    }

    public function getStorageFilesByQuery(string $query): array
    {
        $options = new ListFilesOptions();
        $options->setQuery($query);
        return $this->storageClient->listFiles($options);
    }

}
