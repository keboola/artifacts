<?php

declare(strict_types = 1);

namespace Keboola\Artifacts\Tests;

use Keboola\Artifacts\Artifacts;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;

class ArtifactsTest extends TestCase
{
    public function testUploadCurrent(): void
    {
        $temp = new Temp();
        $tempDir = $temp->getTmpFolder();

        // create some files
        // todo

        // upload them as current artifacts
        $artifacts = $this->getArtifacts($tempDir, 'keboola.component', '123', '456');
        $artifacts->uploadCurrent();
    }

    private function getArtifacts(string $dataDir, string $componentId, string $configId, string $jobId): Artifacts
    {
        $storageClient = new StorageClient([
            'url' => (string) getenv('STORAGE_API_URL'),
            'token' => (string) getenv('STORAGE_API_TOKEN'),
        ]);

        return new Artifacts(
            $storageClient,
            $dataDir,
            $componentId,
            $configId,
            $jobId,
        );
    }
}
