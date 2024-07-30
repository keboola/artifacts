<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests;

use Keboola\Artifacts\Tags;
use PHPUnit\Framework\TestCase;

class TagsTest extends TestCase
{
    public function testToUploadArray(): void
    {
        $tags = new Tags(
            'branchId',
            'componentId',
            'configId',
            'jobId',
            'orchestrationId',
        );
        self::assertSame(
            [
                'artifact',
                'branchId-branchId',
                'componentId-componentId',
                'configId-configId',
                'jobId-jobId',
            ],
            $tags->toUploadArray(),
        );
    }

    public function testMergeConfiguration(): void
    {
        $tags = new Tags(
            'branchId',
            'componentId',
            'configId',
            'jobId',
            'orchestrationId',
        );
        $tagsMerged = $tags::mergeWithConfiguration(
            $tags,
            [
                'branchId' => 'branchId2',
                'componentId' => 'componentId2',
                'configId' => 'configId2',
            ],
        );
        self::assertSame(
            [
                'artifact',
                'branchId-branchId',
                'componentId-componentId',
                'configId-configId',
                'jobId-',
            ],
            $tagsMerged->toUploadArray(),
        );
    }
}
