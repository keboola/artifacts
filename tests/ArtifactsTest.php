<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests;

use Keboola\Artifacts\Artifacts;
use Keboola\Artifacts\Filesystem;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use Symfony\Component\Filesystem\Filesystem as SymfonyFilesystem;
use Symfony\Component\Finder\Finder;

class ArtifactsTest extends TestCase
{
    public function testUploadCurrent(): void
    {
        $temp = new Temp();
        $filesystem = $this->generateArtifacts($temp);

        // upload them as current artifacts
        $storageClient = $this->getStorageClient();
        $logger = new TestLogger();
        $jobId = (string) rand(0, 999999);
        $artifacts = new Artifacts(
            $storageClient,
            $logger,
            $temp,
            'branch-123',
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
        self::assertContains('branchId-branch-123', $storageFile['tags']);
        self::assertContains('componentId-keboola.component', $storageFile['tags']);
        self::assertContains('configId-123', $storageFile['tags']);

        $downloadedArtifactPath = '/tmp/downloaded.tar.gz';
        $storageClient->downloadFile($fileId, $downloadedArtifactPath);
        $filesystem->extractArchive($downloadedArtifactPath, '/tmp');

        $file1 = file_get_contents('/tmp/file1');
        $file2 = file_get_contents('/tmp/folder/file2');
        self::assertEquals('{"foo":"bar"}', $file1);
        self::assertEquals('{"foo":"baz"}', $file2);
    }

    public function testDownloadLatestRuns(): void
    {
        $storageClient = $this->getStorageClient();
        $logger = new TestLogger();

        // generate artifacts for a few jobs
        for ($i=0; $i<10; $i++) {
            $temp = new Temp();
            $this->generateArtifacts($temp);

            $jobId = (string) rand(0, 999999);
            $artifacts = new Artifacts(
                $storageClient,
                $logger,
                $temp,
                'branch-123',
                'keboola.component',
                '123',
                $jobId
            );
            $artifacts->uploadCurrent();
        }
        // another config and component
        for ($i=0; $i<10; $i++) {
            $temp = new Temp();
            $this->generateArtifacts($temp);

            $jobId = (string) rand(0, 999999);
            $artifacts = new Artifacts(
                $storageClient,
                $logger,
                $temp,
                'branch-123',
                'keboola.component-2',
                '456',
                $jobId
            );
            $artifacts->uploadCurrent();
        }

        $temp = new Temp();
        $artifacts = new Artifacts(
            $storageClient,
            $logger,
            $temp,
            'branch-123',
            'keboola.component',
            '123',
            $jobId
        );
        $artifacts->downloadLatestRuns(5);

        // level 1
        $finder = new Finder();
        $finder
            ->files()
            ->in($artifacts->getFilesystem()->getRunsDir())
            ->depth(1)
        ;

        foreach ($finder as $file) {
            self::assertEquals('file1', $file->getFilename());
            self::assertEquals('{"foo":"bar"}', $file->getContents());
        }

        self::assertEquals(5, $finder->count());

        // level 2 (sub folder)
        $finder = new Finder();
        $finder
            ->files()
            ->in($artifacts->getFilesystem()->getRunsDir())
            ->depth(2)
        ;

        foreach ($finder as $file) {
            self::assertEquals('file2', $file->getFilename());
            self::assertEquals('{"foo":"baz"}', $file->getContents());
        }

        self::assertEquals(5, $finder->count());
    }

    private function generateArtifacts(Temp $temp): Filesystem
    {
        $artifactsFilesystem = new Filesystem($temp);

        $filePath1 = $artifactsFilesystem->getCurrentDir() . '/file1';
        $filePath2 = $artifactsFilesystem->getCurrentDir() . '/folder/file2';
        $filesystem = new SymfonyFilesystem();

        // create some files
        $filesystem->dumpFile($filePath1, (string) json_encode([
            'foo' => 'bar',
        ]));

        $filesystem->dumpFile($filePath2, (string) json_encode([
            'foo' => 'baz',
        ]));

        return $artifactsFilesystem;
    }

    private function getStorageClient(): StorageClient
    {
        return new StorageClient([
            'url' => (string) getenv('STORAGE_API_URL'),
            'token' => (string) getenv('STORAGE_API_TOKEN'),
        ]);
    }
}
