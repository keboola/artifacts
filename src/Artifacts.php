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

    public function uploadCurrent(): ?int
    {
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
            return $fileId;
        } catch (ProcessFailedException | ClientException $e) {
            throw new ArtifactsException(sprintf('Error uploading file: %s', $e->getMessage()), 0, $e);
        }
    }

    public function downloadLatestRuns(
        int $limit = 1,
        ?string $dateSince = null
    ): void {
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

        $files = $this->storageClient->listFiles(
            (new ListFilesOptions())
                ->setQuery($query)
                ->setLimit($limit)
        );

        foreach ($files as $file) {
            try {
                $jobId = StorageFileHelper::getJobIdFromFileTag($file);
                $tmpPath = $this->filesystem->getTmpDir() . '/' . $file['id'];
                $this->storageClient->downloadFile($file['id'], $tmpPath);
                $this->filesystem->extractArchive($tmpPath, $this->filesystem->getJobRunDir($jobId));
            } catch (ArtifactsException $e) {
                $this->logger->warning(sprintf(
                    'Error downloading run artifact file id "%s"',
                    $file['id']
                ));
            }
        }
    }
}
