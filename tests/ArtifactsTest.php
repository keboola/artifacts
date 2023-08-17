<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests;

use DateTime;
use Generator;
use Keboola\Artifacts\Artifacts;
use Keboola\Artifacts\ArtifactsException;
use Keboola\Artifacts\Filesystem;
use Keboola\Artifacts\Result;
use Keboola\Artifacts\Tags;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\Temp\Temp;
use Monolog\Logger;
use PHPUnit\Framework\MockObject\Generator\Generator as MockGenerator;
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

        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock
            ->expects($this->never())
            ->method($this->anything())
        ;
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBasicClient')->willReturn($storageClientMock);

        $artifacts = new Artifacts(
            $clientWrapperMock,
            $this->createMock(Logger::class),
            $temp
        );

        self::assertSame(
            $temp->getTmpFolder() . '/tmp',
            $artifacts->getFilesystem()->getTmpDir()
        );
    }

    /** @dataProvider uploadProvider */
    public function testUpload(
        ?string $orchestrationId,
        array $configuration,
        int $expectedCurrentCount,
        int $expectedSharedCount,
        bool $zip = true
    ): void {
        $temp = new Temp();
        $filesystem = new Filesystem($temp);
        $jobId = (string) rand(0, 999999);
        $storageClientWrapper = $this->getStorageClientWrapper();

        // upload the artifacts
        $this->generateArtifacts($temp, self::TYPE_CURRENT);
        $this->generateArtifacts($temp, self::TYPE_SHARED);

        $artifacts = new Artifacts(
            $storageClientWrapper,
            new NullLogger(),
            $temp
        );
        $uploadedFiles = $artifacts->upload(
            new Tags(
                'branch-123',
                'keboola.component',
                '123',
                $jobId,
                $orchestrationId
            ),
            $configuration
        );

        $current = array_filter($uploadedFiles, fn ($item) => !$item->isShared());
        $shared = array_filter($uploadedFiles, fn ($item) => $item->isShared());
        self::assertCount($expectedCurrentCount, $current);
        self::assertCount($expectedSharedCount, $shared);

        // wait for file to be available in Storage
        sleep(1);

        // current file
        $storageFiles = $storageClientWrapper->getBasicClient()->listFiles(
            (new ListFilesOptions())
                ->setQuery(sprintf('tags:(jobId-%d* NOT shared)', $jobId))
        );
        self::assertCount($expectedCurrentCount, $storageFiles);

        $storageFile = current($storageFiles);
        $uploadedFileCurrent = array_pop($current);
        self::assertNotNull($uploadedFileCurrent);
        $this->downloadAndAssertStorageFile($filesystem, $storageFile, $uploadedFileCurrent, [
            'artifact',
            'branchId-branch-123',
            'componentId-keboola.component',
            'configId-123',
        ], $zip);

        // shared file
        $storageFiles = $storageClientWrapper->getBasicClient()->listFiles(
            (new ListFilesOptions())
                ->setQuery(sprintf('tags:(jobId-%d* AND orchestrationId-%s)', $jobId, $orchestrationId))
        );
        self::assertCount($expectedSharedCount, $storageFiles);

        if (!empty($storageFiles)) {
            $storageFile = current($storageFiles);
            $uploadedFileShared = array_shift($shared);
            self::assertNotNull($uploadedFileShared);
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

    public static function uploadProvider(): Generator
    {
        yield 'orchestrationId set' => [
            'orchestrationId' => (string) rand(0, 999999),
            'configuration' => [],
            'currentFilesCount' => 1,
            'sharedFilesCount' => 1,
        ];

        yield 'orchestrationId null' => [
            'orchestrationId' => null,
            'configuration' => [],
            'currentFilesCount' => 1,
            'sharedFilesCount' => 0,
        ];

        yield 'no zip' => [
            'orchestrationId' => null,
            'configuration' => [
                'artifacts' => [
                    'options' => [
                        'zip' => false,
                    ],
                ],
            ],
            'currentFilesCount' => 3,
            'sharedFilesCount' => 0,
            'zip' => false,
        ];
    }

    public static function uploadExceptionsHandlingData(): iterable
    {
        yield 'ProcessFailedException convert' => [
            new ProcessFailedException((new MockGenerator)->getMock(Process::class, callOriginalConstructor: false)),
            ArtifactsException::class,
            'Error uploading file: The command "" failed.',
        ];
        yield 'ClientException convert' => [
            new ClientException('You don\'t have access to the resource.'),
            ArtifactsException::class,
            'Error uploading file: You don\'t have access to the resource.',
        ];
        yield 'random exception do not convert' => [
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
        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock->expects(self::once())
            ->method('uploadFile')
            ->willThrowException($exception);

        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')->willReturn($storageClientMock);

        $temp = new Temp();
        $this->generateArtifacts($temp, self::TYPE_CURRENT);

        $artifacts = new Artifacts(
            $clientWrapperMock,
            new NullLogger(),
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
        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock
            ->expects($this->never())
            ->method($this->anything())
        ;
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBasicClient')->willReturn($storageClientMock);

        $temp = new Temp();
        $artifacts = new Artifacts(
            $clientWrapperMock,
            $this->createMock(Logger::class),
            $temp
        );

        $results = $artifacts->upload(new Tags(
            'main-branch',
            'keboola.orchestrator',
            '123456',
            '123456789'
        ));

        self::assertEmpty($results);
    }

    public function testUploadConfigIdNull(): void
    {
        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock
            ->expects($this->never())
            ->method($this->anything())
        ;
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBasicClient')->willReturn($storageClientMock);

        $testLogger = new TestLogger();

        $temp = new Temp();
        $artifacts = new Artifacts(
            $clientWrapperMock,
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
    public function testDownloadRunsSimple(
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
            5,
            self::TYPE_CURRENT,
            $configuration
        );

        // another branch, config and component
        $this->generateAndUploadArtifacts(
            'default',
            'keboola.component-2',
            '456',
            null,
            5,
            self::TYPE_CURRENT,
            $configuration
        );

        sleep(1);

        $logger = new TestLogger();
        $temp = new Temp();
        $artifacts = new Artifacts(
            $this->getStorageClientWrapper(),
            $logger,
            $temp
        );

        $results = $artifacts->download(new Tags(
            $branchId,
            $componentId,
            $configId,
            (string) rand(0, 999999)
        ), $configuration);

        self::assertCount($expectedCount, $results);

        $downloadDir = empty($configuration['artifacts']['custom']['enabled'])
            ? $artifacts->getFilesystem()->getDownloadRunsDir()
            : $artifacts->getFilesystem()->getDownloadCustomDir();

        $this->assertFilesAndContent($downloadDir);
    }

    public static function downloadRunsProvider(): Generator
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

    /** @dataProvider downloadRunsProviderNoZip */
    public function testDownloadRunsNoZip(
        string $branchId,
        string $componentId,
        string $configId,
        array $configuration,
        array $expectedFiles
    ): void {
        $configuration['artifacts']['options']['zip'] = false;

        // generate artifacts for a few jobs
        $this->generateAndUploadArtifacts(
            'branch-123',
            'keboola.component',
            '123',
            null,
            1,
            self::TYPE_CURRENT,
            $configuration
        );

        // another branch, config and component
        $this->generateAndUploadArtifacts(
            'default',
            'keboola.component-2',
            '456',
            null,
            1,
            self::TYPE_CURRENT,
            $configuration
        );

        sleep(2);
        $logger = new TestLogger();
        $temp = new Temp();
        $artifacts = new Artifacts(
            $this->getStorageClientWrapper(),
            $logger,
            $temp
        );

        $result = $artifacts->download(new Tags(
            $branchId,
            $componentId,
            $configId,
            (string) rand(0, 999999)
        ), $configuration);

        self::assertCount(count($expectedFiles), $result);

        $downloadDir = empty($configuration['artifacts']['custom']['enabled'])
            ? $artifacts->getFilesystem()->getDownloadRunsDir()
            : $artifacts->getFilesystem()->getDownloadCustomDir();

        $this->assertFilesAndContentNoZip($downloadDir, $expectedFiles);
    }

    public static function downloadRunsProviderNoZip(): Generator
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
            'expectedFiles' => [
                'file1' => '{"foo":"bar"}',
                'file2' => '{"foo":"baz"}',
                'file3' => '{"baz":"bar"}',
            ],
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
            'expectedFiles' => [
                'file2' => '{"foo":"baz"}',
                'file3' => '{"baz":"bar"}',
            ],
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
            'expectedFiles' => [
                'file1' => '{"foo":"bar"}',
                'file2' => '{"foo":"baz"}',
                'file3' => '{"baz":"bar"}',
            ],
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
            'expectedFiles' => [
                'file2' => '{"foo":"baz"}',
                'file3' => '{"baz":"bar"}',
            ],
        ];
    }

    public function testDownloadConfigIdNull(): void
    {
        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock
            ->expects($this->never())
            ->method($this->anything())
        ;
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getBasicClient')->willReturn($storageClientMock);

        $testLogger = new TestLogger();

        $artifacts = new Artifacts(
            $clientWrapperMock,
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

        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock
            ->expects(self::once())
            ->method('listFiles')
            ->with((new ListFilesOptions())
                ->setQuery($expectedQuery)
                ->setLimit($expectedLimit))
            ->willReturn([]);
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')->willReturn($storageClientMock);

        $logger = new TestLogger();
        $temp = new Temp();
        $artifacts = new Artifacts(
            $clientWrapperMock,
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

    public static function downloadRunsDateSinceProvider(): Generator
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

        $storageClientMock = $this->createMock(Client::class);
        $storageClientMock
            ->expects(self::once())
            ->method('listFiles')
            ->with((new ListFilesOptions())->setQuery($expectedQuery))
            ->willReturn([]);
        $clientWrapperMock = $this->createMock(ClientWrapper::class);
        $clientWrapperMock->method('getTableAndFileStorageClient')->willReturn($storageClientMock);

        $logger = new TestLogger();
        $temp = new Temp();
        $artifacts = new Artifacts(
            $clientWrapperMock,
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

    public function testUploadUseBranchStorage(): void
    {
        $temp = new Temp();
        $jobId = (string) rand(0, 999999);

        $client = new Client([
            'url' => (string) getenv('STORAGE_API_URL'),
            'token' => (string) getenv('STORAGE_API_TOKEN'),
        ]);
        $branchesApi = new DevBranches($client);
        $branchId = $branchesApi->createBranch(uniqid(__method__))['id'];

        $storageClientWrapper = new ClientWrapper(
            new ClientOptions(
                url: (string) getenv('STORAGE_API_URL'),
                token: (string) getenv('STORAGE_API_TOKEN'),
                useBranchStorage: true,
                branchId: (string) $branchId,
            ),
        );
        // upload the artifacts
        $this->generateArtifacts($temp, self::TYPE_CURRENT);
        $artifacts = new Artifacts($storageClientWrapper, new NullLogger(), $temp);
        $uploadedFiles = $artifacts->upload(
            new Tags(
                (string) $branchId,
                'keboola.component',
                '123',
                $jobId,
                null
            ),
            []
        );

        // wait for file to be available in Storage
        sleep(1);

        $current = array_filter($uploadedFiles, fn ($item) => !$item->isShared());
        self::assertCount(1, $current);
        $fileId = $uploadedFiles[0]->getStorageFileId();

        // verify that the file exists in development branch
        $fileInfo = $storageClientWrapper->getBranchClient()->getFile($fileId);
        self::assertEquals($fileId, $fileInfo['id']);

        try {
            // and does not exist in default branch
            $storageClientWrapper->getClientForDefaultBranch()->getFile($fileId);
            self::fail('Must throw exception');
        } catch (ClientException $e) {
            self::assertStringContainsString('File not found', $e->getMessage());
        }

        $branchesApi->deleteBranch($branchId);
    }

    private function generateAndUploadArtifacts(
        string $branchId,
        string $componentId,
        string $configId,
        ?string $orchestrationId,
        int $count,
        string $type = self::TYPE_CURRENT,
        array $configuration = []
    ): void {
        $storageClient = $this->getStorageClientWrapper();
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
            ), $configuration);
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
        $filePath3 = $uploadDir . '/folder/file3';
        $filesystem = new SymfonyFilesystem();

        // create some files
        $filesystem->dumpFile($filePath1, (string) json_encode([
            'foo' => 'bar',
        ]));

        $filesystem->dumpFile($filePath2, (string) json_encode([
            'foo' => 'baz',
        ]));

        $filesystem->dumpFile($filePath3, (string) json_encode([
            'baz' => 'bar',
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
            $this->getStorageClientWrapper(),
            $logger,
            $temp
        );
        $results = $artifacts->download(
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

        self::assertCount($count, $results);

        $this->assertFilesAndContent($artifacts->getFilesystem()->getDownloadSharedDir());
    }

    private function downloadAndAssertStorageFile(
        Filesystem $filesystem,
        array $storageFile,
        Result $uploadedResult,
        array $tags,
        bool $unzip = true
    ): void {
        $clientWrapper = $this->getStorageClientWrapper();

        self::assertSame($storageFile['id'], $uploadedResult->getStorageFileId());
        foreach ($tags as $tag) {
            self::assertContains($tag, $storageFile['tags']);
        }

        $dir = new Temp('artifacts-');
        $downloadedArtifactPath = $unzip ?
            $dir->getTmpFolder() . '/downloaded.tar.gz' : $dir->getTmpFolder() . '/' . $storageFile['name'];
        $clientWrapper->getBasicClient()->downloadFile($uploadedResult->getStorageFileId(), $downloadedArtifactPath);

        if ($unzip) {
            $filesystem->extractArchive($downloadedArtifactPath, '/tmp');
        }

        $file1 = file_get_contents('/tmp/file1');
        $file2 = file_get_contents('/tmp/folder/file2');
        self::assertSame('{"foo":"bar"}', $file1);
        self::assertSame('{"foo":"baz"}', $file2);
    }

    private function assertFilesAndContent(string $expectedDownloadDir): void
    {
        // level 1
        $finder = new Finder();
        $finder
            ->files()
            ->in($expectedDownloadDir)
            ->depth(1)
            ->sortByName()
        ;

        foreach ($finder as $file) {
            self::assertEquals('file1', $file->getFilename());
            self::assertEquals('{"foo":"bar"}', $file->getContents());
        }

        // level 2 (sub folder)
        $finder = new Finder();
        $finder
            ->files()
            ->in($expectedDownloadDir)
            ->depth(2)
            ->sortByName()
        ;

        $files = iterator_to_array($finder);
        $file = array_shift($files);
        self::assertNotNull($file);
        self::assertEquals('file2', $file->getFilename());
        self::assertEquals('{"foo":"baz"}', $file->getContents());

        $file = array_shift($files);
        self::assertNotNull($file);
        self::assertEquals('file3', $file->getFilename());
        self::assertEquals('{"baz":"bar"}', $file->getContents());
    }

    private function assertFilesAndContentNoZip(
        string $expectedDownloadDir,
        array $expectedFiles
    ): void {
        // level 1
        $finder = new Finder();
        $finder
            ->files()
            ->in($expectedDownloadDir)
            ->depth(1)
            ->sortByName()
        ;

        $files = iterator_to_array($finder);
        foreach ($expectedFiles as $expectedFile => $expectedContent) {
            $file = array_shift($files);
            self::assertNotNull($file);
            self::assertEquals($expectedFile, $file->getFilename());
            self::assertEquals($expectedContent, $file->getContents());
        }
    }

    private function getStorageClientWrapper(): ClientWrapper
    {
        return new ClientWrapper(
            new ClientOptions(
                url: (string) getenv('STORAGE_API_URL'),
                token: (string) getenv('STORAGE_API_TOKEN'),
            ),
        );
    }
}
