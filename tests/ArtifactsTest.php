<?php

declare(strict_types = 1);

namespace Keboola\Artifacts\Tests;

use Keboola\Artifacts\Artifacts;
use Keboola\Artifacts\Filesystem;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;

class ArtifactsTest extends TestCase
{
    public function testUploadCurrent(): void
    {
        $temp = new Temp();
        $tempDir = $temp->getTmpFolder();
        $dataDir = $tempDir . '/data';
        $artifactsFilesystem = new Filesystem($dataDir);

        $filePath1 = $artifactsFilesystem->getRunsCurrentDir() . '/file1';
        $filePath2 = $artifactsFilesystem->getRunsCurrentDir() . '/file2';

        // create some files
        file_put_contents($filePath1, json_encode([
            'foo' => 'bar',
        ]));

        file_put_contents($filePath2, json_encode([
            'foo' => 'baz',
        ]));

        // upload them as current artifacts
        $storageClient = $this->getStorageClient();
        $logger = new TestLogger();
        $jobId = (string) rand(0, 999999);
        $artifacts = new Artifacts(
            $storageClient,
            $logger,
            $dataDir,
            'keboola.component',
            '123',
            $jobId
        );
        $fileId = $artifacts->uploadCurrent();

        $storageFile = $storageClient->listFiles(
            (new ListFilesOptions())
                ->setQuery(sprintf('tags:jobId-%d*', $jobId))
        )[0];

        self::assertEquals($fileId, $storageFile['id']);
        self::assertContains('artifact', $storageFile['tags']);
        self::assertContains('componentId-keboola.component', $storageFile['tags']);
        self::assertContains('configId-123', $storageFile['tags']);
    }

    private function getStorageClient(): StorageClient
    {
        return new StorageClient([
            'url' => (string) getenv('STORAGE_API_URL'),
            'token' => (string) getenv('STORAGE_API_TOKEN'),
        ]);
    }
}
