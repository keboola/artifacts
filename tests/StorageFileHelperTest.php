<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests;

use Keboola\Artifacts\ArtifactsException;
use Keboola\Artifacts\File;
use Keboola\Artifacts\StorageFileHelper;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\Branch;
use Keboola\StorageApiBranch\ClientWrapper;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use PHPUnit\Framework\TestCase;

class StorageFileHelperTest extends TestCase
{
    public function testGetJobIdFromFileTag(): void
    {
        $file = new File(
            id: '123456',
            name: 'test.csv',
            tags: [
                'componentId-keboola.component',
                'configId-123',
                'jobId-123456',
            ],
            sourceBranchId: '1234',
        );

        self::assertEquals('jobId-123456', StorageFileHelper::getJobIdFromFileTag($file));
    }

    public function testGetJobIdFromFileTagMissing(): void
    {
        $file = new File(
            id: '123456',
            name: 'test.csv',
            tags: [
                'componentId-keboola.component',
                'configId-123',
            ],
            sourceBranchId: '1234',
        );

        $this->expectExceptionObject(
            new ArtifactsException('Missing jobId tag on artifact file "123456"')
        );
        StorageFileHelper::getJobIdFromFileTag($file);
    }

    public function testGetJobIdFromFileTagMoreThanOne(): void
    {
        $file = new File(
            id: '123456',
            name: 'test.csv',
            tags: [
                'componentId-keboola.component',
                'configId-123',
                'jobId-123456',
                'jobId-456789',
            ],
            sourceBranchId: '1234',
        );

        $this->expectExceptionObject(
            new ArtifactsException('There is more than one jobId tag on artifact file "123456"')
        );
        StorageFileHelper::getJobIdFromFileTag($file);
    }

    /** @dataProvider listFilesBranchesProvider */
    public function testListFiles(
        array $basicFiles,
        int $basicCalls,
        array $defaultFiles,
        int $defaultCalls,
        bool $isDevelopmentBranch,
        bool $useBranchStorage,
        array $expectedFiles,
    ): void {
        $clientMock = $this->createMock(BranchAwareClient::class);
        $clientMock
            ->expects(self::exactly($basicCalls))
            ->method('listFiles')
            ->willReturn($basicFiles);
        $defaultClientMock = $this->createMock(BranchAwareClient::class);
        $defaultClientMock
            ->expects(self::exactly($defaultCalls))
            ->method('listFiles')
            ->willReturn($defaultFiles);

        $clientWrapper = $this->createMock(ClientWrapper::class);
        $clientWrapper
            ->method('getTableAndFileStorageClient')
            ->willReturn($clientMock);
        $clientWrapper
            ->method('getClientForDefaultBranch')
            ->willReturn($defaultClientMock);
        $clientWrapper->method('isDevelopmentBranch')->willReturn($isDevelopmentBranch);
        $clientOptionsReadOnly = new ClientOptions(
            useBranchStorage: $useBranchStorage,
        );
        $clientWrapper->method('getClientOptionsReadOnly')->willReturn($clientOptionsReadOnly);
        $clientWrapper->method('getBranchId')->willReturn('54321');
        $clientWrapper->method('getDefaultBranch')->willReturn(
            new Branch('12345', 'default', true, null)
        );

        $files = StorageFileHelper::listFiles(
            $clientWrapper,
            new ListFilesOptions(),
        );
        self::assertEquals($expectedFiles, $files);
    }

    public static function listFilesBranchesProvider()
    {
        yield 'run on default branch, no files' => [
            'basicFiles' => [],
            'basicCalls' => 1,
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => false,
            'useBranchStorage' => false,
            'expectedFiles' => [],
        ];
        yield 'run on default branch, some files' => [
            'basicFiles' => [
                [
                    'id' => '123456789',
                    'name' => 'test.csv',
                    'tags' => [
                        'componentId-keboola.component',
                        'configId-123',
                        'jobId-123456',
                    ],
                ],
            ],
            'basicCalls' => 1,
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => false,
            'useBranchStorage' => false,
            'expectedFiles' => [
                new File(
                    id: '123456789',
                    name: 'test.csv',
                    tags: [
                        'componentId-keboola.component',
                        'configId-123',
                        'jobId-123456',
                    ],
                    sourceBranchId: '54321',
                ),
            ],
        ];
        yield 'run on non-default branch, no files' => [
            'basicFiles' => [],
            'basicCalls' => 1,
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => true,
            'useBranchStorage' => false,
            'expectedFiles' => [],
        ];
        yield 'run on non-default branch, some files' => [
            'basicFiles' => [
                [
                    'id' => '123456789',
                    'name' => 'test.csv',
                    'tags' => [
                        'componentId-keboola.component',
                        'configId-123',
                        'branchId-54321',
                        'jobId-123456',
                    ],
                ],
            ],
            'basicCalls' => 1,
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => true,
            'useBranchStorage' => false,
            'expectedFiles' => [
                new File(
                    id: '123456789',
                    name: 'test.csv',
                    tags: [
                        'componentId-keboola.component',
                        'configId-123',
                        'branchId-54321',
                        'jobId-123456',
                    ],
                    sourceBranchId: '54321',
                ),
            ],
        ];
        yield 'run on default branch, no files, real dev storage' => [
            'basicFiles' => [],
            'basicCalls' => 1,
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => false,
            'useBranchStorage' => true,
            'expectedFiles' => [],
        ];
        yield 'run on default branch, some files, real dev storage' => [
            'basicFiles' => [
                [
                    'id' => '123456789',
                    'name' => 'test.csv',
                    'tags' => [
                        'componentId-keboola.component',
                        'configId-123',
                        'jobId-123456',
                    ],
                ],
            ],
            'basicCalls' => 1,
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => false,
            'useBranchStorage' => true,
            'expectedFiles' => [
                new File(
                    id: '123456789',
                    name: 'test.csv',
                    tags: [
                        'componentId-keboola.component',
                        'configId-123',
                        'jobId-123456',
                    ],
                    sourceBranchId: '54321',
                ),
            ],
        ];
        yield 'run on non-default branch, no files, real dev storage' => [
            'basicFiles' => [],
            'basicCalls' => 1,
            'defaultFiles' => [],
            'defaultCalls' => 1,
            'isDevelopmentBranch' => true,
            'useBranchStorage' => true,
            'expectedFiles' => [],
        ];
        yield 'run on non-default branch, some files, real dev storage' => [
            'basicFiles' => [
                [
                    'id' => '123456789',
                    'name' => 'test.csv',
                    'tags' => [
                        'componentId-keboola.component',
                        'configId-123',
                        'branchId-54321',
                        'jobId-123456',
                    ],
                ],
            ],
            'basicCalls' => 1,
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => true,
            'useBranchStorage' => true,
            'expectedFiles' => [
                new File(
                    id: '123456789',
                    name: 'test.csv',
                    tags: [
                        'componentId-keboola.component',
                        'configId-123',
                        'branchId-54321',
                        'jobId-123456',
                    ],
                    sourceBranchId: '54321', // branch id points to dev branch
                ),
            ],
        ];
        yield 'run on non-default branch, no files in branch only, real dev storage' => [
            'basicFiles' => [],
            'basicCalls' => 1,
            'defaultFiles' => [
                [
                    'id' => '123456789',
                    'name' => 'test.csv',
                    'tags' => [
                        'componentId-keboola.component',
                        'configId-123',
                        'branchId-54321',
                        'jobId-123456',
                    ],
                ],
            ],
            'defaultCalls' => 1,
            'isDevelopmentBranch' => true,
            'useBranchStorage' => true,
            'expectedFiles' => [
                new File(
                    id: '123456789',
                    name: 'test.csv',
                    tags: [
                        'componentId-keboola.component',
                        'configId-123',
                        'branchId-54321',
                        'jobId-123456',
                    ],
                    sourceBranchId: '12345', // branch id points to default branch
                ),
            ],
        ];
    }
}
