<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

class File
{
    public function __construct(
        public readonly string $id,
        public readonly string $name,
        public readonly array $tags,
        public readonly string $sourceBranchId,
    ) {
    }
}
