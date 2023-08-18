<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

use DateTime;

class Tags
{
    private string $branchId;
    private string $componentId;
    private ?string $configId;
    private ?string $jobId;
    private ?string $orchestrationId;
    private bool $isShared = false;

    public function __construct(
        string $branchId,
        string $componentId,
        ?string $configId,
        ?string $jobId = null,
        ?string $orchestrationId = null
    ) {
        $this->branchId = $branchId;
        $this->componentId = $componentId;
        $this->configId = $configId;
        $this->jobId = $jobId;
        $this->orchestrationId = $orchestrationId;
    }

    public function getBranchId(): string
    {
        return $this->branchId;
    }

    public function getComponentId(): string
    {
        return $this->componentId;
    }

    public function getConfigId(): ?string
    {
        return $this->configId;
    }

    public function getJobId(): ?string
    {
        return $this->jobId;
    }

    public function getOrchestrationId(): ?string
    {
        return $this->orchestrationId;
    }

    public function setIsShared(bool $value): self
    {
        $this->isShared = $value;
        return $this;
    }

    public function getIsShared(): bool
    {
        return $this->isShared;
    }

    public static function mergeWithConfiguration(Tags $tags, array $filter): self
    {
        return new self(
            $filter['branch_id'] ?? $tags->getBranchId(),
            $filter['component_id'] ?? $tags->getComponentId(),
            $filter['config_id'] ?? $tags->getConfigId(),
        );
    }

    public function toUploadArray(): array
    {
        $result = [
            'artifact',
            'branchId-' . $this->branchId,
            'componentId-' . $this->componentId,
            'configId-' . $this->configId,
            'jobId-' . $this->jobId,
        ];

        if ($this->isShared) {
            array_push($result, 'shared', sprintf('orchestrationId-%s', $this->orchestrationId));
        }

        return $result;
    }
}
