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
    private string $currentDir;
    private string $runsDir;
    private string $archivePath;
    private SymfonyFilesystem $filesystem;

    public function __construct(Temp $temp)
    {
        $this->tmpDir = $temp->getTmpFolder() . '/tmp';
        $this->dataDir = $temp->getTmpFolder() . '/data';
        $this->archivePath = $temp->getTmpFolder() . '/tmp/artifacts.tar.gz';
        $this->artifactsDir = $this->dataDir . '/artifacts';
        $this->currentDir = $this->artifactsDir . '/current';
        $this->runsDir = $this->artifactsDir . '/runs';

        $this->filesystem = new SymfonyFilesystem();
        $this->mkdir($this->tmpDir);
        $this->mkdir($this->artifactsDir);
        $this->mkdir($this->currentDir);
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

    public function getCurrentDir(): string
    {
        return $this->currentDir;
    }

    public function getRunsDir(): string
    {
        return $this->runsDir;
    }

    public function getJobRunDir(string $jobId): string
    {
        return sprintf('%s/%s', $this->runsDir, $jobId);
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
