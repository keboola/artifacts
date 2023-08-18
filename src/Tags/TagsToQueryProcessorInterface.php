<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tags;

use Keboola\Artifacts\Tags;

interface TagsToQueryProcessorInterface
{
    public function toQuery(Tags $tags): string;
}
