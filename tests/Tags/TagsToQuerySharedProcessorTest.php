<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests\Tags;

use Keboola\Artifacts\Tags;
use Keboola\Artifacts\Tags\TagsToQuerySharedProcessor;
use PHPUnit\Framework\TestCase;

class TagsToQuerySharedProcessorTest extends TestCase
{
    public function testToQuery(): void
    {
        $processor = new TagsToQuerySharedProcessor();
        $query = $processor->toQuery(new Tags(
            'branchId',
            'componentId',
            'configId',
            'jobId',
            'orchestrationId'
        ));
        self::assertSame(
            'tags:(artifact AND shared AND branchId-branchId AND orchestrationId-orchestrationId)',
            $query
        );
    }
}
