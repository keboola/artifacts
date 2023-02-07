<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

class Result
{
    public function __construct(
        private readonly int $storageFileId
    ) {
    }

    public function getStorageFileId(): int
    {
        return $this->storageFileId;
    }
}
