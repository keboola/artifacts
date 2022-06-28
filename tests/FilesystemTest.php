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

        $path = $artifactsFilesystem->getCurrentDir();
        self::assertDirectoryExists($path);
        self::assertSame(
            sprintf('%s/data/artifacts/current', $temp->getTmpFolder()),
            $path
        );

        $path = $artifactsFilesystem->getRunsDir();
        self::assertDirectoryDoesNotExist($path);
        self::assertSame(
            sprintf('%s/data/artifacts/runs', $temp->getTmpFolder()),
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

        mkdir($artifactsFilesystem->getCurrentDir() . '/test');
        touch($artifactsFilesystem->getCurrentDir() . '/test1.txt');
        touch($artifactsFilesystem->getCurrentDir() . '/test/test2.txt');

        $artifactsFilesystem->archiveDir($artifactsFilesystem->getCurrentDir(), $artifactsFilesystem->getArchivePath());
        self::assertFileExists($artifactsFilesystem->getArchivePath());

        $targetPath = $temp->getTmpFolder() . '/archive-extract-test';

        $artifactsFilesystem->extractArchive($artifactsFilesystem->getArchivePath(), $targetPath);
        self::assertDirectoryExists($targetPath);

        $finder = (new Finder())->files()->in($targetPath);
        self::assertSame(2, $finder->count());

        $files = [];
        foreach ($finder as $file) {
            $files[] = (string) $file;
        }

        self::assertSame(
            [
                sprintf('%s/archive-extract-test/test1.txt', $temp->getTmpFolder()),
                sprintf('%s/archive-extract-test/test/test2.txt', $temp->getTmpFolder()),
            ],
            $files
        );
    }
}
