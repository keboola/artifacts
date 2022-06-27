<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests;

use Keboola\Artifacts\ArtifactsException;
use Keboola\Artifacts\StorageFileHelper;
use PHPUnit\Framework\TestCase;

class StorageFileHelperTest extends TestCase
{
    public function testGetJobIdFromFileTag(): void
    {
        $file = [
            'id' => 123456,
            'tags' => [
                'componentId-keboola.component',
                'configId-123',
                'jobId-123456',
            ],
        ];

        self::assertEquals('jobId-123456', StorageFileHelper::getJobIdFromFileTag($file));
    }

    public function testGetJobIdFromFileTagMissing(): void
    {
        $file = [
            'id' => 123456,
            'tags' => [
                'componentId-keboola.component',
                'configId-123',
            ],
        ];

        self::expectExceptionObject(
            new ArtifactsException('Missing jobId tag on artifact file "123456"')
        );
        StorageFileHelper::getJobIdFromFileTag($file);
    }

    public function testGetJobIdFromFileTagMoreThanOne(): void
    {
        $file = [
            'id' => 123456,
            'tags' => [
                'componentId-keboola.component',
                'configId-123',
                'jobId-123456',
                'jobId-456789',
            ],
        ];

        self::expectExceptionObject(
            new ArtifactsException('There is more than one jobId tag on artifact file "123456"')
        );
        StorageFileHelper::getJobIdFromFileTag($file);
    }
}
