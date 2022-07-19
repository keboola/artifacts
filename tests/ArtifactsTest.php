<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests;

use DateTime;
use Generator;
use Keboola\Artifacts\Artifacts;
use Keboola\Artifacts\ArtifactsException;
use Keboola\Artifacts\Filesystem;
use Keboola\Artifacts\Tags;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\Temp\Temp;
use Monolog\Logger;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Psr\Log\Test\TestLogger;
use RangeException;
use Symfony\Component\Filesystem\Filesystem as SymfonyFilesystem;
use Symfony\Component\Finder\Finder;
use Symfony\Component\Process\Exception\ProcessFailedException;
use Symfony\Component\Process\Process;
use Throwable;

class ArtifactsTest extends TestCase
{
    private const TYPE_CURRENT = 'current';
    private const TYPE_SHARED = 'shared';

    public function testGetFilesystem(): void
    {
        $temp = new Temp();

        $artifacts = new Artifacts(
            self::createMock(StorageClient::class),
            self::createMock(Logger::class),
            $temp
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
        $jobId = (string) rand(0, 999999);
        $storageClient = $this->getStorageClient();

        // upload the artifacts
        $this->generateArtifacts($temp, self::TYPE_CURRENT);
        $this->generateArtifacts($temp, self::TYPE_SHARED);

        $artifacts = new Artifacts(
            $storageClient,
            new NullLogger(),
            $temp
        );
        $uploadedFiles = $artifacts->upload(new Tags(
            'branch-123',
            'keboola.component',
            '123',
            $jobId,
            $orchestrationId
        ));

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
        $this->generateArtifacts($temp, self::TYPE_CURRENT);

        $artifacts = new Artifacts(
            $storageClientMock,
            self::createMock(Logger::class),
            $temp
        );

        $this->expectException($expectedException);
        $this->expectExceptionMessage($expectedExceptionMessage);

        $artifacts->upload(new Tags(
            'keboola.orchestrator',
            '123456',
            '123456789',
            (string) rand(0, 99999)
        ));
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
            $temp
        );

        self::assertEmpty($artifacts->upload(new Tags(
            'main-branch',
            'keboola.orchestrator',
            '123456',
            '123456789'
        )));
    }

    public function testUploadConfigIdNull(): void
    {
        $storageClientMock = self::createMock(StorageClient::class);
        $storageClientMock
            ->expects($this->never())
            ->method($this->anything())
        ;

        $testLogger = new TestLogger();

        $temp = new Temp();
        $artifacts = new Artifacts(
            $storageClientMock,
            $testLogger,
            $temp
        );

        self::assertEmpty($artifacts->upload(new Tags(
            'main-branch',
            'keboola.orchestrator',
            null,
            '123456789'
        )));
        self::assertTrue($testLogger->hasWarningThatContains(
            'Ignoring artifacts, configuration Id is not set'
        ));
    }

    /** @dataProvider downloadRunsProvider */
    public function testDownloadRuns(
        string $branchId,
        string $componentId,
        string $configId,
        array $configuration,
        int $expectedCount
    ): void {
        // generate artifacts for a few jobs
        $this->generateAndUploadArtifacts(
            'branch-123',
            'keboola.component',
            '123',
            null,
            5
        );

        // another branch, config and component
        $this->generateAndUploadArtifacts(
            'default',
            'keboola.component-2',
            '456',
            null,
            5
        );

        $logger = new TestLogger();
        $temp = new Temp();
        $artifacts = new Artifacts(
            $this->getStorageClient(),
            $logger,
            $temp
        );

        $result = $artifacts->download(new Tags(
            $branchId,
            $componentId,
            $configId,
            (string) rand(0, 999999)
        ), $configuration);

        self::assertCount($expectedCount, $result);
        self::assertArrayHasKey('storageFileId', $result[0]);

        $downloadDir = empty($configuration['artifacts']['custom']['enabled'])
            ? $artifacts->getFilesystem()->getDownloadRunsDir()
            : $artifacts->getFilesystem()->getDownloadCustomDir();

        $this->assertFilesAndContent($downloadDir, $expectedCount);
    }

    public function downloadRunsProvider(): Generator
    {
        yield 'runs' => [
            'branch' => 'branch-123',
            'component' => 'keboola.component',
            'configId' => '123',
            'configuration' => [
                'artifacts' => [
                    'runs' => [
                        'enabled' => true,
                        'filter' => [
                            'limit' => 3,
                            'date_since' => '-1 day',
                        ],
                    ],
                ],
            ],
            'expectedCount' => 3,
        ];

        yield 'runs 2' => [
            'branch' => 'default',
            'component' => 'keboola.component-2',
            'configId' => '456',
            'configuration' => [
                'artifacts' => [
                    'runs' => [
                        'enabled' => true,
                        'filter' => [
                            'limit' => 2,
                            'date_since' => '-1 day',
                        ],
                    ],
                ],
            ],
            'expectedCount' => 2,
        ];

        yield 'custom' => [
            'branch' => 'branch-3',
            'component' => 'branch-3',
            'config' => '789',
            'configuration' => [
                'artifacts' => [
                    'custom' => [
                        'enabled' => true,
                        'filter' => [
                            'branch_id' => 'default',
                            'component_id' => 'keboola.component-2',
                            'config_id' => '456',
                            'limit' => 3,
                            'date_since' => '-1 day',
                        ],
                    ],
                ],
            ],
            'expectedCount' => 3,
        ];

        yield 'custom 2' => [
            'branch' => 'default',
            'component' => 'keboola.component',
            'configId' => '999',
            'configuration' => [
                'artifacts' => [
                    'custom' => [
                        'enabled' => true,
                        'filter' => [
                            'branch_id' => 'branch-123',
                            'component_id' => 'keboola.component',
                            'config_id' => '123',
                            'limit' => 2,
                            'date_since' => '-1 day',
                        ],
                    ],
                ],
            ],
            'expectedCount' => 2,
        ];
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
        );

        self::assertEmpty($artifacts->download(new Tags(
            'main-branch',
            'keboola.orchestrator',
            null,
            '123456789'
        ), []));
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
            $temp
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
        $artifacts->download(new Tags(
            $branchId,
            'keboola.component',
            '123',
            'job-123'
        ), $configuration);
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
            $temp
        );
        $configuration = [
            'artifacts' => [
                'shared' => [
                    'enabled' => true,
                ],
            ],
        ];
        $artifacts->download(new Tags(
            'default',
            'keboola.component',
            '123',
            'job-123',
            '99999'
        ), $configuration);
    }

    public function testDownloadShared(): void
    {
        $orchestrationId = (string) rand(0, 999999);
        $orchestrationId2 = (string) rand(0, 999999);

        // generate shared artifacts for a few jobs
        $this->generateAndUploadArtifacts(
            'default',
            'keboola.component',
            '123',
            $orchestrationId,
            3,
            self::TYPE_SHARED
        );

        // another config and component
        $this->generateAndUploadArtifacts(
            'default',
            'keboola.component-2',
            '456',
            $orchestrationId,
            3,
            self::TYPE_SHARED
        );

        // another branch and orchestrationId
        $this->generateAndUploadArtifacts(
            'branch-123',
            'keboola.component-2',
            '456',
            $orchestrationId2,
            3,
            self::TYPE_SHARED
        );

        // same branch another orchestrationId
        $this->generateAndUploadArtifacts(
            'default',
            'keboola.component-2',
            '456',
            $orchestrationId2,
            3,
            self::TYPE_SHARED
        );

        $this->downloadAndAssertShared('default', $orchestrationId, 6);
        $this->downloadAndAssertShared('branch-123', $orchestrationId2, 3);
    }

    private function generateAndUploadArtifacts(
        string $branchId,
        string $componentId,
        string $configId,
        ?string $orchestrationId,
        int $count,
        string $type = self::TYPE_CURRENT
    ): void {
        $storageClient = $this->getStorageClient();
        for ($i=0; $i<$count; $i++) {
            $temp = new Temp();
            $this->generateArtifacts($temp, $type);

            $artifacts = new Artifacts(
                $storageClient,
                new NullLogger(),
                $temp
            );
            $artifacts->upload(new Tags(
                $branchId,
                $componentId,
                $configId,
                (string) rand(0, 999999),
                $orchestrationId
            ));
        }
    }

    private function generateArtifacts(Temp $temp, string $type): void
    {
        $artifactsFs = new Filesystem($temp);

        $uploadDir = $artifactsFs->getUploadCurrentDir();
        if ($type === self::TYPE_SHARED) {
            $uploadDir = $artifactsFs->getUploadSharedDir();
        }

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
            $temp
        );
        $result = $artifacts->download(
            new Tags(
                $branchId,
                'keboola.some-component',
                'some-config',
                (string) rand(0, 999999),
                $orchestrationId
            ),
            [
                'artifacts' => [
                    'shared' => [
                        'enabled' => true,
                    ],
                ],
            ]
        );

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
