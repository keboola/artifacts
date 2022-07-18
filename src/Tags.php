<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

use Keboola\StorageApiBranch\ClientWrapper;

class Tags
{
    private string $branchId;
    private string $componentId;
    private ?string $configId;
    private ?string $jobId;
    private ?string $orchestrationId;

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

    public function getJobId(): string
    {
        return $this->jobId;
    }

    public function getOrchestrationId(): ?string
    {
        return $this->orchestrationId;
    }

    public static function fromConfiguration(array $configuration = []): self
    {
        $artifactsCustomConfiguration = $configuration['artifacts']['custom'];
        $componentId = $artifactsCustomConfiguration['filter']['component_id'];
        $configId = $artifactsCustomConfiguration['filter']['config_id'];
        $branchId = $artifactsCustomConfiguration['filter']['branch_id'] ?? ClientWrapper::BRANCH_DEFAULT;

        return new self(
            $branchId,
            $componentId,
            $configId
        );
    }

    public function toArray(): array
    {
        $result = [
            'artifact',
            'branchId-' . $this->branchId,
            'componentId-' . $this->componentId,
            'configId-' . $this->configId,
            'jobId-' . $this->jobId,
        ];

        if ($this->orchestrationId) {
            array_push($result, 'shared', sprintf('orchestrationId-%s', $this->orchestrationId));
        }

        return $result;
    }
}
