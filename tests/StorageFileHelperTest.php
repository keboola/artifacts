<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests;

use Generator;
use Keboola\Artifacts\ArtifactsException;
use Keboola\Artifacts\File;
use Keboola\Artifacts\StorageFileHelper;
use Keboola\Artifacts\Tags;
use Keboola\Artifacts\Tags\TagsToQueryProcessorInterface;
use Keboola\Artifacts\Tags\TagsToQueryRunsProcessor;
use Keboola\Artifacts\Tags\TagsToQuerySharedProcessor;
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
        string $tagBranchId,
        array $basicFiles,
        array $defaultFiles,
        int $defaultCalls,
        bool $isDevelopmentBranch,
        bool $useBranchStorage,
        array $expectedFiles,
        ListFilesOptions $expectedListFilesOptionsBasic,
        ?ListFilesOptions $expectedListFilesOptionsDefault,
        TagsToQueryProcessorInterface $tagsToQueryProcessor,
    ): void {
        $clientMock = $this->createMock(BranchAwareClient::class);
        $clientMock
            ->expects(self::once())
            ->method('listFiles')
            ->with($expectedListFilesOptionsBasic)
            ->willReturn($basicFiles);
        $defaultClientMock = $this->createMock(BranchAwareClient::class);
        $defaultClientMock
            ->expects(self::exactly($defaultCalls))
            ->method('listFiles')
            ->with($expectedListFilesOptionsDefault)
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
            new Tags(
                $tagBranchId,
                'keboola.component',
                '123',
                '1234567890',
                null,
            ),
            50,
            $tagsToQueryProcessor,
        );
        self::assertEquals($expectedFiles, $files);
    }

    public static function listFilesBranchesProvider(): Generator
    {
        yield 'run on default branch, no files, shared processor' => [
            'tagBranchId' => '12345',
            'basicFiles' => [],
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => false,
            'useBranchStorage' => false,
            'expectedFiles' => [],
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery('tags:(artifact AND shared AND branchId-12345 AND orchestrationId-)')
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQuerySharedProcessor(),
        ];
        yield 'run on default branch, some files, shared processor' => [
            'tagBranchId' => '12345',
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
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery('tags:(artifact AND shared AND branchId-12345 AND orchestrationId-)')
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQuerySharedProcessor(),
        ];
        yield 'run on non-default branch, no files, shared processor' => [
            'tagBranchId' => '54321',
            'basicFiles' => [],
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => true,
            'useBranchStorage' => false,
            'expectedFiles' => [],
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery('tags:(artifact AND shared AND branchId-54321 AND orchestrationId-)')
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQuerySharedProcessor(),
        ];
        yield 'run on non-default branch, some files, shared processor' => [
            'tagBranchId' => '54321',
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
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery('tags:(artifact AND shared AND branchId-54321 AND orchestrationId-)')
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQuerySharedProcessor(),
        ];
        yield 'run on default branch, no files, real dev storage, shared processor' => [
            'tagBranchId' => '12345',
            'basicFiles' => [],
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => false,
            'useBranchStorage' => true,
            'expectedFiles' => [],
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery('tags:(artifact AND shared AND branchId-12345 AND orchestrationId-)')
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQuerySharedProcessor(),
        ];
        yield 'run on default branch, some files, real dev storage, shared processor' => [
            'tagBranchId' => '12345',
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
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery('tags:(artifact AND shared AND branchId-12345 AND orchestrationId-)')
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQuerySharedProcessor(),
        ];
        yield 'run on non-default branch, no files, real dev storage, shared processor' => [
            'tagBranchId' => '54321',
            'basicFiles' => [],
            'defaultFiles' => [],
            'defaultCalls' => 1,
            'isDevelopmentBranch' => true,
            'useBranchStorage' => true,
            'expectedFiles' => [],
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                // current branch id
                ->setQuery('tags:(artifact AND shared AND branchId-54321 AND orchestrationId-)')
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => (new ListFilesOptions())
                // default branch id
                ->setQuery('tags:(artifact AND shared AND branchId-12345 AND orchestrationId-)')
                ->setLimit(50),
            'tagsToQueryProcessor' => new TagsToQuerySharedProcessor(),
        ];
        yield 'run on non-default branch, some files, real dev storage, shared processor' => [
            'tagBranchId' => '54321',
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
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery('tags:(artifact AND shared AND branchId-54321 AND orchestrationId-)')
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQuerySharedProcessor(),
        ];
        yield 'run on non-default branch, no files in branch only, real dev storage, shared processor' => [
            'tagBranchId' => '54321',
            'basicFiles' => [],
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
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                // current branch id
                ->setQuery('tags:(artifact AND shared AND branchId-54321 AND orchestrationId-)')
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => (new ListFilesOptions())
                // default branch id
                ->setQuery('tags:(artifact AND shared AND branchId-12345 AND orchestrationId-)')
                ->setLimit(50),
            'tagsToQueryProcessor' => new TagsToQuerySharedProcessor(),
        ];

        yield 'run on default branch, no files, runs processor' => [
            'tagBranchId' => '12345',
            'basicFiles' => [],
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => false,
            'useBranchStorage' => false,
            'expectedFiles' => [],
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery(
                    'tags:(artifact AND branchId-12345 AND componentId-keboola.component' .
                    ' AND configId-123 NOT shared) AND created:>2021-01-01'
                )
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQueryRunsProcessor('2021-01-01'),
        ];
        yield 'run on default branch, some files, runs processor' => [
            'tagBranchId' => '12345',
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
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery(
                    'tags:(artifact AND branchId-12345 AND componentId-keboola.component ' .
                    'AND configId-123 NOT shared) AND created:>2021-01-01'
                )
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQueryRunsProcessor('2021-01-01'),
        ];
        yield 'run on non-default branch, no files, runs processor' => [
            'tagBranchId' => '54321',
            'basicFiles' => [],
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => true,
            'useBranchStorage' => false,
            'expectedFiles' => [],
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery(
                    'tags:(artifact AND branchId-54321 AND componentId-keboola.component ' .
                    'AND configId-123 NOT shared) AND created:>2021-01-01'
                )
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQueryRunsProcessor('2021-01-01'),
        ];
        yield 'run on non-default branch, some files, runs processor' => [
            'tagBranchId' => '54321',
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
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery(
                    'tags:(artifact AND branchId-54321 AND componentId-keboola.component ' .
                    'AND configId-123 NOT shared) AND created:>2021-01-01'
                )
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQueryRunsProcessor('2021-01-01'),
        ];
        yield 'run on default branch, no files, real dev storage, runs processor' => [
            'tagBranchId' => '12345',
            'basicFiles' => [],
            'defaultFiles' => [],
            'defaultCalls' => 0,
            'isDevelopmentBranch' => false,
            'useBranchStorage' => true,
            'expectedFiles' => [],
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery(
                    'tags:(artifact AND branchId-12345 AND componentId-keboola.component ' .
                    'AND configId-123 NOT shared) AND created:>2021-01-01'
                )
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQueryRunsProcessor('2021-01-01'),
        ];
        yield 'run on default branch, some files, real dev storage, runs processor' => [
            'tagBranchId' => '12345',
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
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery(
                    'tags:(artifact AND branchId-12345 AND componentId-keboola.component ' .
                    'AND configId-123 NOT shared) AND created:>2021-01-01'
                )
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQueryRunsProcessor('2021-01-01'),
        ];
        yield 'run on non-default branch, no files, real dev storage, runs processor' => [
            'tagBranchId' => '54321',
            'basicFiles' => [],
            'defaultFiles' => [],
            'defaultCalls' => 1,
            'isDevelopmentBranch' => true,
            'useBranchStorage' => true,
            'expectedFiles' => [],
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                // current branch id
                ->setQuery(
                    'tags:(artifact AND branchId-54321 AND componentId-keboola.component ' .
                    'AND configId-123 NOT shared) AND created:>2021-01-01'
                )
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => (new ListFilesOptions())
                // default branch id
                ->setQuery(
                    'tags:(artifact AND branchId-12345 AND componentId-keboola.component ' .
                    'AND configId-123 NOT shared) AND created:>2021-01-01'
                )
                ->setLimit(50),
            'tagsToQueryProcessor' => new TagsToQueryRunsProcessor('2021-01-01'),
        ];
        yield 'run on non-default branch, some files, real dev storage, runs processor' => [
            'tagBranchId' => '54321',
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
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                ->setQuery(
                    'tags:(artifact AND branchId-54321 AND componentId-keboola.component AND ' .
                    'configId-123 NOT shared) AND created:>2021-01-01'
                )
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => null,
            'tagsToQueryProcessor' => new TagsToQueryRunsProcessor('2021-01-01'),
        ];
        yield 'run on non-default branch, no files in branch only, real dev storage, runs processor' => [
            'tagBranchId' => '54321',
            'basicFiles' => [],
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
            'expectedListFilesOptionsBasic' => (new ListFilesOptions())
                // current branch id
                ->setQuery(
                    'tags:(artifact AND branchId-54321 AND componentId-keboola.component ' .
                    'AND configId-123 NOT shared) AND created:>2021-01-01'
                )
                ->setLimit(50),
            'expectedListFilesOptionsDefault' => (new ListFilesOptions())
                // default branch id
                ->setQuery(
                    'tags:(artifact AND branchId-12345 AND componentId-keboola.component ' .
                    'AND configId-123 NOT shared) AND created:>2021-01-01'
                )
                ->setLimit(50),
            'tagsToQueryProcessor' => new TagsToQueryRunsProcessor('2021-01-01'),
        ];
    }
}
