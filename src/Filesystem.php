<?php

declare(strict_types = 1);

namespace Keboola\Artifacts;

use Symfony\Component\Filesystem\Filesystem as SymfonyFilesystem;

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

    private function mkdir(string $path): void
    {
        if (!$this->filesystem->exists($path)) {
            $this->filesystem->mkdir($path);
        }
    }
}
