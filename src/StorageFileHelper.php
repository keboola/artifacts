<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

class StorageFileHelper
{
    public static function getJobIdFromFileTag(array $file): string
    {
        $jobIds = array_filter($file['tags'], function ($tag) {
            return strstr($tag, 'jobId');
        });

        if (empty($jobIds)) {
            throw new ArtifactsException(
                sprintf('Missing jobId tag on artifact file "%s"', $file['id'])
            );
        }

        if (count($jobIds) > 1) {
            throw new ArtifactsException(
                sprintf('There is more than one jobId tag on artifact file "%s"', $file['id'])
            );
        }

        return array_shift($jobIds);
    }
}
