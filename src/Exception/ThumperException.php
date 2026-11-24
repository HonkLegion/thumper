<?php

declare(strict_types=1);

namespace Thumper\Exception;

use Spiral\Goridge\RPC\Exception\ServiceException;

/**
 * Base class of the errors the plugin reports for RabbitMQ operations.
 *
 * Extends goridge's ServiceException, which the client throws for every other plugin error.
 */
class ThumperException extends ServiceException
{
    /**
     * Builds the exception for an error reply of the plugin: a kind, a code and a reason.
     * The code is the AMQP reply code for the "amqp" kind, and 0 otherwise.
     */
    public static function create(string $kind, int $code, string $reason): self
    {
        $class = match ($kind) {
            'amqp' => match ($code) {
                403 => AccessRefusedException::class,
                404 => NotFoundException::class,
                405 => ResourceLockedException::class,
                406 => PreconditionFailedException::class,
                default => AmqpException::class,
            },
            'nack' => PublishNackedException::class,
            'timeout' => TimeoutException::class,
            'connection' => ConnectionException::class,
            default => self::class,
        };

        return new $class($reason, $code);
    }
}
