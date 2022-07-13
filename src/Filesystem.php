<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

use Keboola\Temp\Temp;
use Symfony\Component\Filesystem\Filesystem as SymfonyFilesystem;
use Symfony\Component\Process\Process;

class Filesystem
{
    private string $tmpDir;
    private string $dataDir;
    private string $artifactsDir;
    private string $uploadDir;
    private string $uploadCurrentDir;
    private string $uploadSharedDir;
    private string $downloadDir;
    private string $downloadRunsDir;
    private string $downloadSharedDir;
    private string $downloadCustomDir;
    private string $archivePath;
    private SymfonyFilesystem $filesystem;

    public function __construct(Temp $temp)
    {
        $this->tmpDir = $temp->getTmpFolder() . '/tmp';
        $this->dataDir = $temp->getTmpFolder() . '/data';
        $this->archivePath = $temp->getTmpFolder() . '/tmp/artifacts.tar.gz';
        $this->artifactsDir = $this->dataDir . '/artifacts';
        $this->uploadDir = $this->artifactsDir . '/upload';
        $this->uploadCurrentDir = $this->uploadDir . '/current';
        $this->uploadSharedDir = $this->uploadDir . '/shared';
        $this->downloadDir = $this->artifactsDir . '/download';
        $this->downloadRunsDir = $this->downloadDir . '/runs';
        $this->downloadSharedDir = $this->downloadDir . '/shared';
        $this->downloadCustomDir = $this->downloadDir . '/custom';

        $this->filesystem = new SymfonyFilesystem();
        $this->mkdir($this->tmpDir);
        $this->mkdir($this->artifactsDir);
        $this->mkdir($this->uploadCurrentDir);
        $this->mkdir($this->uploadSharedDir);
    }

    public function getTmpDir(): string
    {
        return $this->tmpDir;
    }

    public function getDataDir(): string
    {
        return $this->dataDir;
    }

    public function getArtifactsDir(): string
    {
        return $this->artifactsDir;
    }

    public function getUploadCurrentDir(): string
    {
        return $this->uploadCurrentDir;
    }

    public function getUploadSharedDir(): string
    {
        return $this->uploadSharedDir;
    }

    public function getDownloadRunsDir(): string
    {
        return $this->downloadRunsDir;
    }

    public function getDownloadRunsJobDir(string $jobId): string
    {
        return sprintf('%s/%s', $this->downloadRunsDir, $jobId);
    }

    public function getDownloadSharedDir(): string
    {
        return $this->downloadSharedDir;
    }

    public function getDownloadCustomDir(): string
    {
        return $this->downloadCustomDir;
    }

    public function getArchivePath(): string
    {
        return $this->archivePath;
    }

    public function archiveDir(string $sourcePath, string $targetPath): void
    {
        $process = new Process([
            'tar',
            '-C',
            $sourcePath,
            '-czvf',
            $targetPath,
            '.',
        ]);
        $process->mustRun();
    }

    public function extractArchive(string $sourcePath, string $targetPath): void
    {
        $this->mkdir($targetPath);
        $process = new Process([
            'tar',
            '-xf',
            $sourcePath,
            '-C',
            $targetPath,
        ]);
        $process->mustRun();
    }

    private function mkdir(string $path): void
    {
        if (!$this->filesystem->exists($path)) {
            $this->filesystem->mkdir($path);
        }
    }
}
