<?php

declare(strict_types=1);

namespace Keboola\Artifacts\Tests;

use Keboola\Artifacts\Filesystem as ArtifactsFilesystem;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Finder\Finder;

class FilesystemTest extends TestCase
{
    public function testFilesystemInitsDirectoriesStructure(): void
    {
        $temp = new Temp();
        $artifactsFilesystem = new ArtifactsFilesystem($temp);

        $path = $artifactsFilesystem->getTmpDir();
        self::assertDirectoryExists($path);
        self::assertSame(
            sprintf('%s/tmp', $temp->getTmpFolder()),
            $path
        );

        $path = $artifactsFilesystem->getDataDir();
        self::assertDirectoryExists($path);
        self::assertSame(
            sprintf('%s/data', $temp->getTmpFolder()),
            $path
        );

        $path = $artifactsFilesystem->getArtifactsDir();
        self::assertDirectoryExists($path);
        self::assertSame(
            sprintf('%s/data/artifacts', $temp->getTmpFolder()),
            $path
        );

        $path = $artifactsFilesystem->getUploadCurrentDir();
        self::assertDirectoryExists($path);
        self::assertSame(
            sprintf('%s/data/artifacts/out/current', $temp->getTmpFolder()),
            $path
        );

        $path = $artifactsFilesystem->getUploadSharedDir();
        self::assertDirectoryExists($path);
        self::assertSame(
            sprintf('%s/data/artifacts/out/shared', $temp->getTmpFolder()),
            $path
        );

        $path = $artifactsFilesystem->getDownloadRunsDir();
        self::assertDirectoryDoesNotExist($path);
        self::assertSame(
            sprintf('%s/data/artifacts/in/runs', $temp->getTmpFolder()),
            $path
        );

        $path = $artifactsFilesystem->getDownloadRunsJobDir('123');
        self::assertDirectoryDoesNotExist($path);
        self::assertSame(
            sprintf('%s/data/artifacts/in/runs/123', $temp->getTmpFolder()),
            $path
        );

        $path = $artifactsFilesystem->getDownloadSharedDir();
        self::assertDirectoryDoesNotExist($path);
        self::assertSame(
            sprintf('%s/data/artifacts/in/shared', $temp->getTmpFolder()),
            $path
        );

        $path = $artifactsFilesystem->getDownloadSharedJobsDir('1234');
        self::assertDirectoryDoesNotExist($path);
        self::assertSame(
            sprintf('%s/data/artifacts/in/shared/1234', $temp->getTmpFolder()),
            $path
        );

        $path = $artifactsFilesystem->getDownloadCustomDir();
        self::assertDirectoryDoesNotExist($path);
        self::assertSame(
            sprintf('%s/data/artifacts/in/custom', $temp->getTmpFolder()),
            $path
        );

        $path = $artifactsFilesystem->getArchivePath();
        self::assertFileDoesNotExist($path);
        self::assertSame(
            sprintf('%s/tmp/artifacts.tar.gz', $temp->getTmpFolder()),
            $path
        );
    }

    public function testArchiveAndExtractDir(): void
    {
        $temp = new Temp();
        $artifactsFilesystem = new ArtifactsFilesystem($temp);

        mkdir($artifactsFilesystem->getUploadCurrentDir() . '/test');
        touch($artifactsFilesystem->getUploadCurrentDir() . '/test1.txt');
        touch($artifactsFilesystem->getUploadCurrentDir() . '/test/test2.txt');

        $artifactsFilesystem->archiveDir(
            $artifactsFilesystem->getUploadCurrentDir(),
            $artifactsFilesystem->getArchivePath()
        );
        self::assertFileExists($artifactsFilesystem->getArchivePath());

        $targetPath = $temp->getTmpFolder() . '/archive-extract-test';

        $artifactsFilesystem->extractArchive($artifactsFilesystem->getArchivePath(), $targetPath);
        self::assertDirectoryExists($targetPath);

        $finder = (new Finder())->files()->in($targetPath)->sortByName();
        self::assertSame(2, $finder->count());

        $files = [];
        foreach ($finder as $file) {
            $files[] = (string) $file;
        }

        self::assertSame(
            [
                sprintf('%s/archive-extract-test/test/test2.txt', $temp->getTmpFolder()),
                sprintf('%s/archive-extract-test/test1.txt', $temp->getTmpFolder()),
            ],
            $files
        );
    }
}
