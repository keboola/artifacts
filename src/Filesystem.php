<?php

declare(strict_types = 1);

namespace Keboola\Artifacts;

use Symfony\Component\Filesystem\Filesystem as SymfonyFilesystem;
use Symfony\Component\Process\Process;

class Filesystem
{
    private string $artifactsDir;
    private string $runsCurrentDir;
    private SymfonyFilesystem $filesystem;

    public function __construct(string $dataDir)
    {
        $this->artifactsDir = $dataDir . 'artifacts/';
        $this->filesystem = new SymfonyFilesystem();

        $this->mkdir($this->artifactsDir);
        $this->runsCurrentDir = sprintf('%sruns/current/', $this->artifactsDir);
        $this->mkdir($this->runsCurrentDir);
    }

    public function getArtifactsDir(): string
    {
        return $this->artifactsDir;
    }

    public function getRunsCurrentDir(): string
    {
        return $this->runsCurrentDir;
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
