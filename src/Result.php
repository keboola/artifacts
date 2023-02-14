<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

class Result
{
    public function __construct(
        private readonly int $storageFileId,
        private readonly bool $isShared = false
    ) {
    }

    public function getStorageFileId(): int
    {
        return $this->storageFileId;
    }

    public function isShared(): bool
    {
        return $this->isShared;
    }
}
