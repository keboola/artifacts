<?php

declare(strict_types=1);

use Keboola\StorageApi\Client;
use Keboola\StorageApi\Exception;
use Symfony\Component\Dotenv\Dotenv;

date_default_timezone_set('Europe/Prague');

require __DIR__ . '/../vendor/autoload.php';

if (file_exists(dirname(__DIR__).'/.env.local')) {
    (new Dotenv())->usePutenv(true)->bootEnv(dirname(__DIR__).'/.env.local', 'dev', []);
}

$requiredEnvs = ['STORAGE_API_URL', 'STORAGE_API_TOKEN'];
foreach ($requiredEnvs as $env) {
    if (empty(getenv($env))) {
        throw new Exception(sprintf('Environment variable "%s" is empty', $env));
    }
}

$client = new Client(['url' => getenv('STORAGE_API_URL'), 'token' => getenv('STORAGE_API_TOKEN')]);
$tokenInfo = $client->verifyToken();
print(sprintf(
    'Authorized as "%s (%s)" to project "%s (%s)" at "%s" stack.',
    $tokenInfo['description'],
    $tokenInfo['id'],
    $tokenInfo['owner']['name'],
    $tokenInfo['owner']['id'],
    $client->getApiUrl()
));
