<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests\Tags;

use Keboola\Artifacts\Tags;
use Keboola\Artifacts\Tags\TagsToQueryRunsProcessor;
use PHPUnit\Framework\TestCase;

class TagsToQueryRunsProcessorTest extends TestCase
{
    public function testToQuery(): void
    {
        $processor = new TagsToQueryRunsProcessor('2023-08-18');
        $query = $processor->toQuery(new Tags(
            'branchId',
            'componentId',
            'configId',
            'jobId',
            'orchestrationId',
        ));
        self::assertSame(
            'tags:(artifact AND branchId-branchId AND componentId-componentId ' .
            'AND configId-configId NOT shared) AND created:>2023-08-18',
            $query,
        );
    }
}
