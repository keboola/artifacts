<?php

declare(strict_types=1);

namespace Keboola\Artifacts;

use Keboola\StorageApi\Options\ListFilesOptions;
use Keboola\StorageApiBranch\ClientWrapper;

class StorageFileHelper
{
    public static function getJobIdFromFileTag(File $file): string
    {
        $jobIds = array_filter($file->tags, function ($tag) {
            return strstr($tag, 'jobId');
        });

        if (empty($jobIds)) {
            throw new ArtifactsException(
                sprintf('Missing jobId tag on artifact file "%s"', $file->id)
            );
        }

        if (count($jobIds) > 1) {
            throw new ArtifactsException(
                sprintf('There is more than one jobId tag on artifact file "%s"', $file->id)
            );
        }

        return array_shift($jobIds);
    }

    public static function listFiles(ClientWrapper $clientWrapper, ListFilesOptions $listFilesOptions): array
    {
        /*
        For fake dev/prod mode, we need to use the default branch client, because there are no files in storage
            branches.
        For real dev/prod mode, we need to use the branched client, because there may be files in storage branches.
        That's what getTableAndFileStorageClient() does.

        If the list is empty then we try to list the file again on the default branch, but only if
            - we're currently on the development branch, otherwise it would repeat the same call when the whole thing
                    is run in default branch
            - we're in real dev/prod mode, because in fake dev/prod mode there is no prod-branch fallback in
                artifacts (they are distinguished by the branchId tag), so it would again repeat the same
                call to list files (theoretically getClientForDefaultBranch and getTableAndFileStorageClient
                    return the same client in this case (practically one is Client and one is BranchAwareClient class,
                    but they work the same in this case))
        */
        $sourceBranchId = $clientWrapper->getBranchId();
        $files = $clientWrapper->getTableAndFileStorageClient()->listFiles($listFilesOptions);
        if (!$files && $clientWrapper->isDevelopmentBranch() &&
            $clientWrapper->getClientOptionsReadOnly()->useBranchStorage()
        ) {
            $files = $clientWrapper->getClientForDefaultBranch()->listFiles($listFilesOptions);
            if ($files) {
                // if something was found in the prod branch, then switch reading to the prod branch
                $sourceBranchId = $clientWrapper->getDefaultBranch()->id;
            }
        }
        return array_map(
            fn ($file) => new File((string) $file['id'], $file['name'], $file['tags'], $sourceBranchId),
            $files
        );
    }
}
