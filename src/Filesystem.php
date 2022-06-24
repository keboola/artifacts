<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

use Keboola\Temp\Temp;
use Symfony\Component\Filesystem\Filesystem as SymfonyFilesystem;
use Symfony\Component\Process\Process;

class Filesystem
{
    private string $dataDir;
    private string $artifactsDir;
    private string $runsCurrentDir;
    private string $archivePath;
    private SymfonyFilesystem $filesystem;

    public function __construct(Temp $temp)
    {
        $this->dataDir = $temp->getTmpFolder() . '/data';
        $this->artifactsDir = $this->dataDir . '/artifacts';
        $this->runsCurrentDir = $this->artifactsDir . '/runs/current/';
        $this->archivePath = $temp->getTmpFolder() . '/tmp/artifacts.tar.gz';
        $tmpDir = $temp->getTmpFolder() . '/tmp';

        $this->filesystem = new SymfonyFilesystem();
        $this->mkdir($tmpDir);
        $this->mkdir($this->artifactsDir);
        $this->mkdir($this->runsCurrentDir);
    }

    public function getDataDir(): string
    {
        return $this->dataDir;
    }

    public function getArtifactsDir(): string
    {
        return $this->artifactsDir;
    }

    public function getRunsCurrentDir(): string
    {
        return $this->runsCurrentDir;
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
