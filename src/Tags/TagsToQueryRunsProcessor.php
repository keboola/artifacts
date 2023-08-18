<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tags;

use DateTime;
use Keboola\Artifacts\Tags;

class TagsToQueryRunsProcessor implements TagsToQueryProcessorInterface
{
    private ?string $dateSince;

    public function __construct(?string $dateSince)
    {
        $this->dateSince = $dateSince;
    }

    public function toQuery(Tags $tags): string
    {
        $query = sprintf(
            'tags:(artifact AND branchId-%s AND componentId-%s AND configId-%s NOT shared)',
            $tags->getBranchId(),
            $tags->getComponentId(),
            $tags->getConfigId()
        );

        if ($this->dateSince) {
            $dateUTC = (new DateTime($this->dateSince))->format('Y-m-d');
            $query .= ' AND created:>' . $dateUTC;
        }

        return $query;
    }
}
