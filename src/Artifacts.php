<?php

declare(strict_types = 1);

namespace Keboola\Artifacts;

use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\Options\FileUploadOptions;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Process;

class Artifacts
{
    private StorageClient $storageClient;
    private Filesystem $filesystem;
    private string $componentId;
    private string $configId;
    private string $jobId;

    public function __construct(
        StorageClient $storageClient,
        string $dataDir,
        string $componentId,
        string $configId,
        string $jobId
    ) {
        $this->storageClient = $storageClient;
        $this->filesystem = new Filesystem($dataDir);
        $this->componentId = $componentId;
        $this->configId = $configId;
        $this->jobId = $jobId;
    }

    public function uploadCurrent(): ?int
    {
        $currentDir = $this->filesystem->getRunsCurrentDir();
        $archivePath = '/tmp/artefacts.tar.gz';

        $finder = new Finder();
        $count = $finder->in($currentDir)->count();

        if ($count === 0) {
            return null;
        }

        $process = new Process([
            'tar',
            '-C',
            $currentDir,
            '-czvf',
            $archivePath,
            '.',
        ]);
        $process->mustRun();

        $options = new FileUploadOptions();
        $options->setTags([
            'artifact',
            'componentId-' . $this->componentId,
            'configId-' . $this->configId,
            'jobId-' . $this->jobId,
        ]);
        return $this->storageClient->uploadFile($archivePath, $options);
    }
}
