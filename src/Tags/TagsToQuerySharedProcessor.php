<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tags;

use Keboola\Artifacts\Tags;

class TagsToQuerySharedProcessor implements TagsToQueryProcessorInterface
{
    public function toQuery(Tags $tags): string
    {
        return sprintf(
            'tags:(artifact AND shared AND branchId-%s AND orchestrationId-%s)',
            $tags->getBranchId(),
            $tags->getOrchestrationId()
        );
    }
}
