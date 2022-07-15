<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests;

use DateTime;
use Generator;
use Keboola\Artifacts\Artifacts;
use Keboola\Artifacts\ArtifactsException;
use Keboola\Artifacts\Filesystem;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\Temp\Temp;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\Test\TestLogger;
use RangeException;
use Symfony\Component\Filesystem\Filesystem as SymfonyFilesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;
use Throwable;

class ArtifactsTest extends TestCase
{
    public function testGetFilesystem(): void
    {
        $temp = new Temp();

        $artifacts = new Artifacts(
            self::createMock(StorageClient::class),
            self::createMock(Logger::class),
            $temp,
            'main-branch',
            'keboola.orchestrator',
            '123456',
            '123456789',
        );

        self::assertSame(
            $temp->getTmpFolder() . '/tmp',
            $artifacts->getFilesystem()->getTmpDir()
        );
    }

    /** @dataProvider uploadProvider */
    public function testUpload(?string $orchestrationId, int $uploadedFilesCount, int $expectedSharedCount): void
    {
        $temp = new Temp();
        $filesystem = new Filesystem($temp);
        $this->generateArtifacts($filesystem->getUploadCurrentDir());
        $this->generateArtifacts($filesystem->getUploadSharedDir());

        // upload the artifacts
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
            $jobId,
            $orchestrationId
        );
        $uploadedFiles = $artifacts->upload();
        self::assertCount($uploadedFilesCount, $uploadedFiles);
        $uploadedFileCurrent = array_shift($uploadedFiles);
        $uploadedFileShared = array_shift($uploadedFiles);

        // wait for file to be available in Storage
        sleep(1);

        // current file
        $storageFiles = $storageClient->listFiles(
            (new ListFilesOptions())
                ->setQuery(sprintf('tags:(jobId-%d* NOT shared)', $jobId))
        );
        self::assertCount(1, $storageFiles);
        $storageFile = current($storageFiles);

        $this->downloadAndAssertStorageFile($filesystem, $storageFile, $uploadedFileCurrent, [
            'artifact',
            'branchId-branch-123',
            'componentId-keboola.component',
            'configId-123',
        ]);

        // shared file
        $storageFiles = $storageClient->listFiles(
            (new ListFilesOptions())
                ->setQuery(sprintf('tags:(jobId-%d* AND orchestrationId-%s)', $jobId, $orchestrationId))
        );
        self::assertCount($expectedSharedCount, $storageFiles);

        if (!empty($storageFiles)) {
            $storageFile = current($storageFiles);
            $this->downloadAndAssertStorageFile($filesystem, $storageFile, $uploadedFileShared, [
                'artifact',
                'branchId-branch-123',
                'componentId-keboola.component',
                'configId-123',
                sprintf('orchestrationId-%s', $orchestrationId),
                'shared',
            ]);
        }
    }

    public function uploadProvider(): Generator
    {
        yield 'orchestrationId set' => [
            'orchestrationId' => (string) rand(0, 999999),
            'uploadedFilesCount' => 2,
            'sharedFilesCount' => 1,
        ];

        yield 'orchestrationId null' => [
            'orchestrationId' => null,
            'uploadedFilesCount' => 1,
            'sharedFilesCount' => 0,
        ];
    }

    public function uploadExceptionsHandlingData(): iterable
    {
        yield 'ProcessFailedException convert' => [
            new ProcessFailedException(self::createMock(Process::class)),
            ArtifactsException::class,
            'Error uploading file: The command "" failed.',
        ];
        yield 'ClientException convert' => [
            new ClientException('You don\'t have access to the resource.'),
            ArtifactsException::class,
            'Error uploading file: You don\'t have access to the resource.',
        ];
        yield 'random expection do not convert' => [
            new RangeException('Test'),
            RangeException::class,
            'Test',
        ];
    }

    /**
     * @dataProvider uploadExceptionsHandlingData
     * @param class-string<Throwable> $expectedException
     */
    public function testUploadExceptionsHandling(
        Throwable $exception,
        string $expectedException,
        string $expectedExceptionMessage
    ): void {
        $storageClientMock = self::createMock(StorageClient::class);
        $storageClientMock->expects(self::once())
            ->method('uploadFile')
            ->willThrowException($exception)
        ;

        $temp = new Temp();
        $this->generateArtifacts((new Filesystem($temp))->getUploadCurrentDir());

        $artifacts = new Artifacts(
            $storageClientMock,
            self::createMock(Logger::class),
            $temp,
            'main-branch',
            'keboola.orchestrator',
            '123456',
            '123456789',
        );

        $this->expectException($expectedException);
        $this->expectExceptionMessage($expectedExceptionMessage);

        $artifacts->upload();
    }

    public function testUploadDoNotUploadIfNoFileExists(): void
    {
        $storageClientMock = self::createMock(StorageClient::class);
        $storageClientMock
            ->expects($this->never())
            ->method($this->anything())
        ;

        $temp = new Temp();
        $artifacts = new Artifacts(
            $storageClientMock,
            self::createMock(Logger::class),
            $temp,
            'main-branch',
            'keboola.orchestrator',
            '123456',
            '123456789',
        );

        self::assertEmpty($artifacts->upload());
    }

    public function testUploadConfigIdNull(): void
    {
        $storageClientMock = self::createMock(StorageClient::class);
        $storageClientMock
            ->expects($this->never())
            ->method($this->anything())
        ;

        $testLogger = new TestLogger();

        $artifacts = new Artifacts(
            $storageClientMock,
            $testLogger,
            new Temp(),
            'main-branch',
            'keboola.orchestrator',
            null,
            '123456789',
        );

        self::assertEmpty($artifacts->upload());
        self::assertTrue($testLogger->hasWarningThatContains(
            'Ignoring artifacts, configuration Id is not set'
        ));
    }

    public function testDownloadRuns(): void
    {
        $storageClient = $this->getStorageClient();
        $logger = new TestLogger();

        // generate artifacts for a few jobs
        for ($i=0; $i<10; $i++) {
            $temp = new Temp();
            $this->generateArtifacts((new Filesystem($temp))->getUploadCurrentDir());

            $artifacts = new Artifacts(
                $storageClient,
                $logger,
                $temp,
                'branch-123',
                'keboola.component',
                '123',
                (string) rand(0, 999999)
            );
            $artifacts->upload();
        }
        // another branch, config and component
        for ($i=0; $i<10; $i++) {
            $temp = new Temp();
            $this->generateArtifacts((new Filesystem($temp))->getUploadCurrentDir());

            $artifacts = new Artifacts(
                $storageClient,
                $logger,
                $temp,
                'default',
                'keboola.component-2',
                '456',
                (string) rand(0, 999999)
            );
            $artifacts->upload();
        }

        $this->downloadAndAssertRuns('branch-123', 'keboola.component', '123', 5);

        $this->downloadAndAssertRuns('default', 'keboola.component-2', '456', 3);
    }

    public function testDownloadConfigIdNull(): void
    {
        $storageClientMock = self::createMock(StorageClient::class);
        $storageClientMock
            ->expects($this->never())
            ->method($this->anything())
        ;

        $testLogger = new TestLogger();

        $artifacts = new Artifacts(
            $storageClientMock,
            $testLogger,
            new Temp(),
            'main-branch',
            'keboola.orchestrator',
            null,
            '123456789',
        );

        self::assertEmpty($artifacts->download([]));
        self::assertTrue($testLogger->hasWarningThatContains(
            'Ignoring artifacts, configuration Id is not set'
        ));
    }

    /**
     * @dataProvider downloadRunsDateSinceProvider
     */
    public function testDownloadRunsDateSince(
        string $createdSince,
        string $branchId,
        ?int $limit,
        int $expectedLimit
    ): void {
        $expectedQuery = sprintf(
            'tags:(artifact AND branchId-%s AND componentId-%s AND configId-%s NOT shared) AND created:>%s',
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
        $configuration = [
            'artifacts' => [
                'runs' => [
                    'enabled' => true,
                    'filter' => [
                        'limit' => $limit,
                        'date_since' => $createdSince,
                    ],
                ],
            ],
        ];
        $artifacts->download($configuration);
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

    public function testDownloadSharedMock(): void
    {
        $expectedQuery = sprintf(
            'tags:(artifact AND shared AND branchId-%s AND orchestrationId-%s)',
            'default',
            '99999'
        );

        $storageClientMock = self::createMock(StorageClient::class);
        $storageClientMock
            ->expects(self::once())
            ->method('listFiles')
            ->with((new ListFilesOptions())->setQuery($expectedQuery))
            ->willReturn([]);

        $logger = new TestLogger();
        $temp = new Temp();
        $artifacts = new Artifacts(
            $storageClientMock,
            $logger,
            $temp,
            'default',
            'keboola.component',
            '123',
            'job-123',
            '99999'
        );
        $configuration = [
            'artifacts' => [
                'shared' => [
                    'enabled' => true,
                ],
            ],
        ];
        $artifacts->download($configuration);
    }

    public function testDownloadShared(): void
    {
        $orchestrationId = (string) rand(0, 999999);
        $orchestrationId2 = (string) rand(0, 999999);
        $storageClient = $this->getStorageClient();
        $logger = new TestLogger();

        // generate shared artifacts for a few jobs
        for ($i=0; $i<3; $i++) {
            $temp = new Temp();
            $this->generateArtifacts((new Filesystem($temp))->getUploadSharedDir());

            $artifacts = new Artifacts(
                $storageClient,
                $logger,
                $temp,
                'default',
                'keboola.component',
                '123',
                (string) rand(0, 999999),
                $orchestrationId
            );
            $artifacts->upload();
        }
        // another config and component
        for ($i=0; $i<3; $i++) {
            $temp = new Temp();
            $this->generateArtifacts((new Filesystem($temp))->getUploadSharedDir());

            $artifacts = new Artifacts(
                $storageClient,
                $logger,
                $temp,
                'default',
                'keboola.component-2',
                '456',
                (string) rand(0, 999999),
                $orchestrationId
            );
            $artifacts->upload();
        }

        // another branch and orchestrationId
        for ($i=0; $i<3; $i++) {
            $temp = new Temp();
            $this->generateArtifacts((new Filesystem($temp))->getUploadSharedDir());

            $artifacts = new Artifacts(
                $storageClient,
                $logger,
                $temp,
                'branch-123',
                'keboola.component-2',
                '456',
                (string) rand(0, 999999),
                $orchestrationId2
            );
            $artifacts->upload();
        }

        // same branch another orchestrationId
        for ($i=0; $i<3; $i++) {
            $temp = new Temp();
            $this->generateArtifacts((new Filesystem($temp))->getUploadSharedDir());

            $artifacts = new Artifacts(
                $storageClient,
                $logger,
                $temp,
                'default',
                'keboola.component-2',
                '456',
                (string) rand(0, 999999),
                $orchestrationId2
            );
            $artifacts->upload();
        }

        $this->downloadAndAssertShared('default', $orchestrationId, 6);
        $this->downloadAndAssertShared('branch-123', $orchestrationId2, 3);
    }

    private function generateArtifacts(string $uploadDir): void
    {
        $filePath1 = $uploadDir . '/file1';
        $filePath2 = $uploadDir . '/folder/file2';
        $filesystem = new SymfonyFilesystem();

        // create some files
        $filesystem->dumpFile($filePath1, (string) json_encode([
            'foo' => 'bar',
        ]));

        $filesystem->dumpFile($filePath2, (string) json_encode([
            'foo' => 'baz',
        ]));
    }

    private function downloadAndAssertRuns(string $branchId, string $componentId, string $configId, int $limit): void
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
        $configuration = [
            'artifacts' => [
                'runs' => [
                    'enabled' => true,
                    'filter' => [
                        'limit' => $limit,
                        'date_since' => '-1 day',
                    ],
                ],
            ],
        ];
        $result = $artifacts->download($configuration);

        self::assertCount($limit, $result);
        self::assertArrayHasKey('storageFileId', $result[0]);

        $this->assertFilesAndContent($artifacts->getFilesystem()->getDownloadRunsDir(), $limit);
    }

    private function downloadAndAssertShared(
        string $branchId,
        string $orchestrationId,
        int $count
    ): void {
        $logger = new TestLogger();
        $temp = new Temp();
        $artifacts = new Artifacts(
            $this->getStorageClient(),
            $logger,
            $temp,
            $branchId,
            'keboola.some-component',
            'some-config',
            (string) rand(0, 999999),
            $orchestrationId
        );
        $result = $artifacts->download([
            'artifacts' => [
                'shared' => [
                    'enabled' => true,
                ],
            ],
        ]);

        self::assertCount($count, $result);
        self::assertArrayHasKey('storageFileId', $result[0]);

        $this->assertFilesAndContent($artifacts->getFilesystem()->getDownloadSharedDir(), $count);
    }

    private function downloadAndAssertStorageFile(
        Filesystem $filesystem,
        array $storageFile,
        array $uploadedStorageFile,
        array $tags
    ): void {
        $storageClient = $this->getStorageClient();

        self::assertSame(['storageFileId' => $storageFile['id']], $uploadedStorageFile);
        self::assertSame($uploadedStorageFile['storageFileId'], $storageFile['id']);
        foreach ($tags as $tag) {
            self::assertContains($tag, $storageFile['tags']);
        }

        $downloadedArtifactPath = '/tmp/downloaded.tar.gz';
        $storageClient->downloadFile($uploadedStorageFile['storageFileId'], $downloadedArtifactPath);
        $filesystem->extractArchive($downloadedArtifactPath, '/tmp');

        $file1 = file_get_contents('/tmp/file1');
        $file2 = file_get_contents('/tmp/folder/file2');
        self::assertSame('{"foo":"bar"}', $file1);
        self::assertSame('{"foo":"baz"}', $file2);
    }

    private function assertFilesAndContent(string $expectedDownloadDir, int $limit): void
    {
        // level 1
        $finder = new Finder();
        $finder
            ->files()
            ->in($expectedDownloadDir)
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
            ->in($expectedDownloadDir)
            ->depth(2)
        ;

        foreach ($finder as $file) {
            self::assertEquals('file2', $file->getFilename());
            self::assertEquals('{"foo":"baz"}', $file->getContents());
        }

        self::assertEquals($limit, $finder->count());
    }

    private function getStorageClient(): StorageClient
    {
        return new StorageClient([
            'url' => (string) getenv('STORAGE_API_URL'),
            'token' => (string) getenv('STORAGE_API_TOKEN'),
        ]);
    }
}
