import { randomBytes, createHash } from "crypto";
import { EventEmitter } from "events";
// AbortController became available as a global in node version 16. Once version
// 14 reaches its end-of-life, this can be removed.
import { AbortController as PolyfillAbortController } from "node-abort-controller";
// Define script constants.
const ACQUIRE_SCRIPT = `
  -- Return 0 if an entry already exists.
  for i, key in ipairs(KEYS) do
    if redis.call("exists", key) == 1 then
      return 0
    end
  end

  -- Create an entry for each provided key.
  for i, key in ipairs(KEYS) do
    redis.call("set", key, ARGV[1], "PX", ARGV[2])
  end

  -- Return the number of entries added.
  return #KEYS
`;
const EXTEND_SCRIPT = `
  -- Return 0 if an entry exists with a *different* lock value.
  for i, key in ipairs(KEYS) do
    if redis.call("get", key) ~= ARGV[1] then
      return 0
    end
  end

  -- Update the entry for each provided key.
  for i, key in ipairs(KEYS) do
    redis.call("set", key, ARGV[1], "PX", ARGV[2])
  end

  -- Return the number of entries updated.
  return #KEYS
`;
const RELEASE_SCRIPT = `
  local count = 0
  for i, key in ipairs(KEYS) do
    -- Only remove entries for *this* lock value.
    if redis.call("get", key) == ARGV[1] then
      redis.pcall("del", key)
      count = count + 1
    end
  end

  -- Return the number of entries removed.
  return count
`;
// Define default settings.
const defaultSettings = {
    driftFactor: 0.01,
    retryCount: 10,
    retryDelay: 200,
    retryJitter: 100,
    automaticExtensionThreshold: 500,
};
// Modifyng this object is forbidden.
Object.freeze(defaultSettings);
/*
 * This error indicates a failure due to the existence of another lock for one
 * or more of the requested resources.
 */
export class ResourceLockedError extends Error {
    constructor(message) {
        super();
        this.message = message;
        this.name = "ResourceLockedError";
    }
}
/*
 * This error indicates a failure of an operation to pass with a quorum.
 */
export class ExecutionError extends Error {
    constructor(message, attempts) {
        super();
        this.message = message;
        this.attempts = attempts;
        this.name = "ExecutionError";
    }
}
/*
 * An object of this type is returned when a resource is successfully locked. It
 * contains convenience methods `release` and `extend` which perform the
 * associated Redlock method on itself.
 */
export class Lock {
    constructor(redlock, resources, value, attempts, expiration) {
        this.redlock = redlock;
        this.resources = resources;
        this.value = value;
        this.attempts = attempts;
        this.expiration = expiration;
    }
    async release() {
        return this.redlock.release(this);
    }
    async extend(duration) {
        return this.redlock.extend(this, duration);
    }
}
/**
 * A redlock object is instantiated with an array of at least one redis client
 * and an optional `options` object. Properties of the Redlock object should NOT
 * be changed after it is first used, as doing so could have unintended
 * consequences for live locks.
 */
export default class Redlock extends EventEmitter {
    constructor(clients, settings = {}, scripts = {}) {
        super();
        // Prevent crashes on error events.
        this.on("error", () => {
            // Because redlock is designed for high availability, it does not care if
            // a minority of redis instances/clusters fail at an operation.
            //
            // However, it can be helpful to monitor and log such cases. Redlock emits
            // an "error" event whenever it encounters an error, even if the error is
            // ignored in its normal operation.
            //
            // This function serves to prevent node's default behavior of crashing
            // when an "error" event is emitted in the absence of listeners.
        });
        // Create a new array of client, to ensure no accidental mutation.
        this.clients = new Set(clients);
        if (this.clients.size === 0) {
            throw new Error("Redlock must be instantiated with at least one redis client.");
        }
        // Customize the settings for this instance.
        this.settings = {
            driftFactor: typeof settings.driftFactor === "number"
                ? settings.driftFactor
                : defaultSettings.driftFactor,
            retryCount: typeof settings.retryCount === "number"
                ? settings.retryCount
                : defaultSettings.retryCount,
            retryDelay: typeof settings.retryDelay === "number"
                ? settings.retryDelay
                : defaultSettings.retryDelay,
            retryJitter: typeof settings.retryJitter === "number"
                ? settings.retryJitter
                : defaultSettings.retryJitter,
            automaticExtensionThreshold: typeof settings.automaticExtensionThreshold === "number"
                ? settings.automaticExtensionThreshold
                : defaultSettings.automaticExtensionThreshold,
        };
        // Use custom scripts and script modifiers.
        const acquireScript = typeof scripts.acquireScript === "function"
            ? scripts.acquireScript(ACQUIRE_SCRIPT)
            : ACQUIRE_SCRIPT;
        const extendScript = typeof scripts.extendScript === "function"
            ? scripts.extendScript(EXTEND_SCRIPT)
            : EXTEND_SCRIPT;
        const releaseScript = typeof scripts.releaseScript === "function"
            ? scripts.releaseScript(RELEASE_SCRIPT)
            : RELEASE_SCRIPT;
        this.scripts = {
            acquireScript: {
                value: acquireScript,
                hash: this._hash(acquireScript),
            },
            extendScript: {
                value: extendScript,
                hash: this._hash(extendScript),
            },
            releaseScript: {
                value: releaseScript,
                hash: this._hash(releaseScript),
            },
        };
    }
    /**
     * Generate a sha1 hash compatible with redis evalsha.
     */
    _hash(value) {
        return createHash("sha1").update(value).digest("hex");
    }
    /**
     * Generate a cryptographically random string.
     */
    _random() {
        return randomBytes(16).toString("hex");
    }
    /**
     * This method runs `.quit()` on all client connections.
     */
    async quit() {
        const results = [];
        for (const client of this.clients) {
            results.push(client.quit());
        }
        await Promise.all(results);
    }
    /**
     * This method acquires a locks on the resources for the duration specified by
     * the `duration`.
     */
    async acquire(resources, duration, settings) {
        var _a;
        if (Math.floor(duration) !== duration) {
            throw new Error("Duration must be an integer value in milliseconds.");
        }
        const start = Date.now();
        const value = this._random();
        try {
            const { attempts } = await this._execute(this.scripts.acquireScript, resources, [value, duration], settings);
            // Add 2 milliseconds to the drift to account for Redis expires precision,
            // which is 1 ms, plus the configured allowable drift factor.
            const drift = Math.round(((_a = settings === null || settings === void 0 ? void 0 : settings.driftFactor) !== null && _a !== void 0 ? _a : this.settings.driftFactor) * duration) + 2;
            return new Lock(this, resources, value, attempts, start + duration - drift);
        }
        catch (error) {
            // If there was an error acquiring the lock, release any partial lock
            // state that may exist on a minority of clients.
            await this._execute(this.scripts.releaseScript, resources, [value], {
                retryCount: 0,
            }).catch(() => {
                // Any error here will be ignored.
            });
            throw error;
        }
    }
    /**
     * This method unlocks the provided lock from all servers still persisting it.
     * It will fail with an error if it is unable to release the lock on a quorum
     * of nodes, but will make no attempt to restore the lock in the case of a
     * failure to release. It is safe to re-attempt a release or to ignore the
     * error, as the lock will automatically expire after its timeout.
     */
    async release(lock, settings) {
        // Immediately invalidate the lock.
        lock.expiration = 0;
        // Attempt to release the lock.
        return this._execute(this.scripts.releaseScript, lock.resources, [lock.value], settings);
    }
    /**
     * This method extends a valid lock by the provided `duration`.
     */
    async extend(existing, duration, settings) {
        var _a;
        if (Math.floor(duration) !== duration) {
            throw new Error("Duration must be an integer value in milliseconds.");
        }
        const start = Date.now();
        // The lock has already expired.
        if (existing.expiration < Date.now()) {
            throw new ExecutionError("Cannot extend an already-expired lock.", []);
        }
        const { attempts } = await this._execute(this.scripts.extendScript, existing.resources, [existing.value, duration], settings);
        // Invalidate the existing lock.
        existing.expiration = 0;
        // Add 2 milliseconds to the drift to account for Redis expires precision,
        // which is 1 ms, plus the configured allowable drift factor.
        const drift = Math.round(((_a = settings === null || settings === void 0 ? void 0 : settings.driftFactor) !== null && _a !== void 0 ? _a : this.settings.driftFactor) * duration) + 2;
        const replacement = new Lock(this, existing.resources, existing.value, attempts, start + duration - drift);
        return replacement;
    }
    /**
     * Execute a script on all clients. The resulting promise is resolved or
     * rejected as soon as this quorum is reached; the resolution or rejection
     * will contains a `stats` property that is resolved once all votes are in.
     */
    async _execute(script, keys, args, _settings) {
        const settings = _settings
            ? {
                ...this.settings,
                ..._settings,
            }
            : this.settings;
        // For the purpose of easy config serialization, we treat a retryCount of
        // -1 a equivalent to Infinity.
        const maxAttempts = settings.retryCount === -1 ? Infinity : settings.retryCount + 1;
        const attempts = [];
        while (true) {
            const { vote, stats } = await this._attemptOperation(script, keys, args);
            attempts.push(stats);
            // The operation achieved a quorum in favor.
            if (vote === "for") {
                return { attempts };
            }
            // Wait before reattempting.
            if (attempts.length < maxAttempts) {
                await new Promise((resolve) => {
                    setTimeout(resolve, Math.max(0, settings.retryDelay +
                        Math.floor((Math.random() * 2 - 1) * settings.retryJitter)), undefined);
                });
            }
            else {
                throw new ExecutionError("The operation was unable to achieve a quorum during its retry window.", attempts);
            }
        }
    }
    async _attemptOperation(script, keys, args) {
        return await new Promise((resolve) => {
            const clientResults = [];
            for (const client of this.clients) {
                clientResults.push(this._attemptOperationOnClient(client, script, keys, args));
            }
            const stats = {
                membershipSize: clientResults.length,
                quorumSize: Math.floor(clientResults.length / 2) + 1,
                votesFor: new Set(),
                votesAgainst: new Map(),
            };
            let done;
            const statsPromise = new Promise((resolve) => {
                done = () => resolve(stats);
            });
            // This is the expected flow for all successful and unsuccessful requests.
            const onResultResolve = (clientResult) => {
                switch (clientResult.vote) {
                    case "for":
                        stats.votesFor.add(clientResult.client);
                        break;
                    case "against":
                        stats.votesAgainst.set(clientResult.client, clientResult.error);
                        break;
                }
                // A quorum has determined a success.
                if (stats.votesFor.size === stats.quorumSize) {
                    resolve({
                        vote: "for",
                        stats: statsPromise,
                    });
                }
                // A quorum has determined a failure.
                if (stats.votesAgainst.size === stats.quorumSize) {
                    resolve({
                        vote: "against",
                        stats: statsPromise,
                    });
                }
                // All votes are in.
                if (stats.votesFor.size + stats.votesAgainst.size ===
                    stats.membershipSize) {
                    done();
                }
            };
            // This is unexpected and should crash to prevent undefined behavior.
            const onResultReject = (error) => {
                throw error;
            };
            for (const result of clientResults) {
                result.then(onResultResolve, onResultReject);
            }
        });
    }
    async _attemptOperationOnClient(client, script, keys, args) {
        try {
            let result;
            try {
                // Attempt to evaluate the script by its hash.
                const shaResult = await client.evalsha(script.hash, keys.length, ...keys, ...args);
                if (typeof shaResult !== "number") {
                    throw new Error(`Unexpected result of type ${typeof shaResult} returned from redis.`);
                }
                result = shaResult;
            }
            catch (error) {
                // If the redis server does not already have the script cached,
                // reattempt the request with the script's raw text.
                if (!(error instanceof Error) ||
                    !error.message.startsWith("NOSCRIPT")) {
                    throw error;
                }
                const rawResult = await client.eval(script.value, keys.length, ...keys, ...args);
                if (typeof rawResult !== "number") {
                    throw new Error(`Unexpected result of type ${typeof rawResult} returned from redis.`);
                }
                result = rawResult;
            }
            // One or more of the resources was already locked.
            if (result !== keys.length) {
                throw new ResourceLockedError(`The operation was applied to: ${result} of the ${keys.length} requested resources.`);
            }
            return {
                vote: "for",
                client,
                value: result,
            };
        }
        catch (error) {
            if (!(error instanceof Error)) {
                throw new Error(`Unexpected type ${typeof error} thrown with value: ${error}`);
            }
            // Emit the error on the redlock instance for observability.
            this.emit("error", error);
            return {
                vote: "against",
                client,
                error,
            };
        }
    }
    async using(resources, duration, settingsOrRoutine, optionalRoutine) {
        if (Math.floor(duration) !== duration) {
            throw new Error("Duration must be an integer value in milliseconds.");
        }
        const settings = settingsOrRoutine && typeof settingsOrRoutine !== "function"
            ? {
                ...this.settings,
                ...settingsOrRoutine,
            }
            : this.settings;
        const routine = optionalRoutine !== null && optionalRoutine !== void 0 ? optionalRoutine : settingsOrRoutine;
        if (typeof routine !== "function") {
            throw new Error("INVARIANT: routine is not a function.");
        }
        if (settings.automaticExtensionThreshold > duration - 100) {
            throw new Error("A lock `duration` must be at least 100ms greater than the `automaticExtensionThreshold` setting.");
        }
        // The AbortController/AbortSignal pattern allows the routine to be notified
        // of a failure to extend the lock, and subsequent expiration. In the event
        // of an abort, the error object will be made available at `signal.error`.
        const controller = typeof AbortController === "undefined"
            ? new PolyfillAbortController()
            : new AbortController();
        const signal = controller.signal;
        function queue() {
            timeout = setTimeout(() => (extension = extend()), lock.expiration - Date.now() - settings.automaticExtensionThreshold);
        }
        async function extend() {
            timeout = undefined;
            try {
                lock = await lock.extend(duration);
                queue();
            }
            catch (error) {
                if (!(error instanceof Error)) {
                    throw new Error(`Unexpected thrown ${typeof error}: ${error}.`);
                }
                if (lock.expiration > Date.now()) {
                    return (extension = extend());
                }
                signal.error = error instanceof Error ? error : new Error(`${error}`);
                controller.abort();
            }
        }
        let timeout;
        let extension;
        let lock = await this.acquire(resources, duration, settings);
        queue();
        try {
            return await routine(signal);
        }
        finally {
            // Clean up the timer.
            if (timeout) {
                clearTimeout(timeout);
                timeout = undefined;
            }
            // Wait for an in-flight extension to finish.
            if (extension) {
                await extension.catch(() => {
                    // An error here doesn't matter at all, because the routine has
                    // already completed, and a release will be attempted regardless. The
                    // only reason for waiting here is to prevent possible contention
                    // between the extension and release.
                });
            }
            await lock.release();
        }
    }
}
//# sourceMappingURL=index.js.map