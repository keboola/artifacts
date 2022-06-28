<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests;

use DateTime;
use Generator;
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

        // wait for file to be available in Storage
        sleep(3);

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

            $artifacts = new Artifacts(
                $storageClient,
                $logger,
                $temp,
                'branch-123',
                'keboola.component',
                '123',
                (string) rand(0, 999999)
            );
            $artifacts->uploadCurrent();
        }
        // another branch, config and component
        for ($i=0; $i<10; $i++) {
            $temp = new Temp();
            $this->generateArtifacts($temp);

            $artifacts = new Artifacts(
                $storageClient,
                $logger,
                $temp,
                'default',
                'keboola.component-2',
                '456',
                (string) rand(0, 999999)
            );
            $artifacts->uploadCurrent();
        }

        $this->downloadAndAssert('branch-123', 'keboola.component', '123', 5);

        $this->downloadAndAssert('default', 'keboola.component-2', '456', 3);
    }

    private function downloadAndAssert(string $branchId, string $componentId, string $configId, int $limit): void
    {
        $logger = new TestLogger();
        $temp = new Temp();
        $artifacts = new Artifacts(
            $this->getStorageClient(),
            $logger,
            $temp,
            $branchId,
            $componentId,
            $configId,
            (string) rand(0, 999999)
        );
        $artifacts->downloadLatestRuns($limit, '-1 day');

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

        self::assertEquals($limit, $finder->count());

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

        self::assertEquals($limit, $finder->count());
    }

    /**
     * @dataProvider downloadRunsDateSinceProvider
     */
    public function testDownloadLatestRunsDateSince(
        string $createdSince,
        string $branchId,
        ?int $limit,
        int $expectedLimit
    ): void {
        $expectedQuery = sprintf(
            'tags:(artifact AND branchId-%s AND componentId-%s AND configId-%s) AND created:>%s',
            $branchId,
            'keboola.component',
            '123',
            (new DateTime($createdSince))->format('Y-m-d')
        );

        $storageClientMock = self::createMock(StorageClient::class);
        $storageClientMock
            ->expects(self::once())
            ->method('listFiles')
            ->with((new ListFilesOptions())
                ->setQuery($expectedQuery)
                ->setLimit($expectedLimit))
            ->willReturn([]);

        $logger = new TestLogger();
        $temp = new Temp();
        $artifacts = new Artifacts(
            $storageClientMock,
            $logger,
            $temp,
            $branchId,
            'keboola.component',
            '123',
            'job-123'
        );
        $artifacts->downloadLatestRuns($limit, $createdSince);
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

    public function downloadRunsDateSinceProvider(): Generator
    {
        yield 'basic' => [
            '2022-01-01',
            'branch-123',
            null,
            Artifacts::DOWNLOAD_FILES_MAX_LIMIT,
        ];

        yield 'relative date' => [
            '-7 days',
            'branch-123',
            null,
            Artifacts::DOWNLOAD_FILES_MAX_LIMIT,
        ];

        yield 'default branch' => [
            'yesterday',
            'default',
            null,
            Artifacts::DOWNLOAD_FILES_MAX_LIMIT,
        ];

        yield 'with limit' => [
            'yesterday',
            'default',
            5,
            5,
        ];

        yield 'with limit over bounds' => [
            'yesterday',
            'default',
            999,
            50,
        ];
    }
}
