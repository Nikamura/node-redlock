"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const util_1 = require("util");
const ava_1 = __importDefault(require("ava"));
const ioredis_1 = __importStar(require("ioredis"));
const index_js_1 = __importStar(require("./index.js"));
async function fail(t, error) {
    if (!(error instanceof index_js_1.ExecutionError)) {
        throw error;
    }
    t.fail(`${error.message}
---
${(await Promise.all(error.attempts))
        .map((s, i) => `ATTEMPT ${i}: ${(0, util_1.formatWithOptions)({ colors: true }, {
        membershipSize: s.membershipSize,
        quorumSize: s.quorumSize,
        votesForSize: s.votesFor.size,
        votesAgainstSize: s.votesAgainst.size,
        votesAgainstError: s.votesAgainst.values(),
    })}`)
        .join("\n\n")}
`);
}
async function waitForCluster(redis) {
    async function checkIsReady() {
        var _a;
        return (((_a = (await redis.cluster("INFO")).match(/^cluster_state:(.+)$/m)) === null || _a === void 0 ? void 0 : _a[1]) === "ok");
    }
    let isReady = await checkIsReady();
    while (!isReady) {
        console.log("Waiting for cluster to be ready...");
        await new Promise((resolve) => setTimeout(resolve, 1000));
        isReady = await checkIsReady();
    }
    async function checkIsWritable() {
        try {
            return (await redis.set("isWritable", "true")) === "OK";
        }
        catch (error) {
            console.error(`Cluster unable to receive writes: ${error}`);
            return false;
        }
    }
    let isWritable = await checkIsWritable();
    while (!isWritable) {
        console.log("Waiting for cluster to be writable...");
        await new Promise((resolve) => setTimeout(resolve, 1000));
        isWritable = await checkIsWritable();
    }
}
function run(namespace, redis) {
    ava_1.default.before(async () => {
        await (redis instanceof ioredis_1.Cluster && redis.isCluster
            ? waitForCluster(redis)
            : null);
    });
    ava_1.default.before(async () => {
        await redis
            .keys("*")
            .then((keys) => ((keys === null || keys === void 0 ? void 0 : keys.length) ? redis.del(keys) : null));
    });
    (0, ava_1.default)(`${namespace} - refuses to use a non-integer duration`, async (t) => {
        try {
            const redlock = new index_js_1.default([redis]);
            const duration = Number.MAX_SAFE_INTEGER / 10;
            // Acquire a lock.
            await redlock.acquire(["{redlock}float"], duration);
            t.fail("Expected the function to throw.");
        }
        catch (error) {
            t.is(error.message, "Duration must be an integer value in milliseconds.");
        }
    });
    (0, ava_1.default)(`${namespace} - acquires, extends, and releases a single lock`, async (t) => {
        try {
            const redlock = new index_js_1.default([redis]);
            const duration = Math.floor(Number.MAX_SAFE_INTEGER / 10);
            // Acquire a lock.
            let lock = await redlock.acquire(["{redlock}a"], duration);
            t.is(await redis.get("{redlock}a"), lock.value, "The lock value was incorrect.");
            t.is(Math.floor((await redis.pttl("{redlock}a")) / 200), Math.floor(duration / 200), "The lock expiration was off by more than 200ms");
            // Extend the lock.
            lock = await lock.extend(3 * duration);
            t.is(await redis.get("{redlock}a"), lock.value, "The lock value was incorrect.");
            t.is(Math.floor((await redis.pttl("{redlock}a")) / 200), Math.floor((3 * duration) / 200), "The lock expiration was off by more than 200ms");
            // Release the lock.
            await lock.release();
            t.is(await redis.get("{redlock}a"), null);
        }
        catch (error) {
            fail(t, error);
        }
    });
    (0, ava_1.default)(`${namespace} - acquires, extends, and releases a multi-resource lock`, async (t) => {
        try {
            const redlock = new index_js_1.default([redis]);
            const duration = Math.floor(Number.MAX_SAFE_INTEGER / 10);
            // Acquire a lock.
            let lock = await redlock.acquire(["{redlock}a1", "{redlock}a2"], duration);
            t.is(await redis.get("{redlock}a1"), lock.value, "The lock value was incorrect.");
            t.is(await redis.get("{redlock}a2"), lock.value, "The lock value was incorrect.");
            t.is(Math.floor((await redis.pttl("{redlock}a1")) / 200), Math.floor(duration / 200), "The lock expiration was off by more than 200ms");
            t.is(Math.floor((await redis.pttl("{redlock}a2")) / 200), Math.floor(duration / 200), "The lock expiration was off by more than 200ms");
            // Extend the lock.
            lock = await lock.extend(3 * duration);
            t.is(await redis.get("{redlock}a1"), lock.value, "The lock value was incorrect.");
            t.is(await redis.get("{redlock}a2"), lock.value, "The lock value was incorrect.");
            t.is(Math.floor((await redis.pttl("{redlock}a1")) / 200), Math.floor((3 * duration) / 200), "The lock expiration was off by more than 200ms");
            t.is(Math.floor((await redis.pttl("{redlock}a2")) / 200), Math.floor((3 * duration) / 200), "The lock expiration was off by more than 200ms");
            // Release the lock.
            await lock.release();
            t.is(await redis.get("{redlock}a1"), null);
            t.is(await redis.get("{redlock}a2"), null);
        }
        catch (error) {
            fail(t, error);
        }
    });
    (0, ava_1.default)(`${namespace} - locks fail when redis is unreachable`, async (t) => {
        var _a, _b;
        try {
            const redis = new ioredis_1.default({
                host: "127.0.0.1",
                maxRetriesPerRequest: 0,
                autoResendUnfulfilledCommands: false,
                autoResubscribe: false,
                retryStrategy: () => null,
                reconnectOnError: () => false,
            });
            redis.on("error", () => {
                // ignore redis-generated errors
            });
            const redlock = new index_js_1.default([redis]);
            const duration = Math.floor(Number.MAX_SAFE_INTEGER / 10);
            try {
                await redlock.acquire(["{redlock}b"], duration);
                throw new Error("This lock should not be acquired.");
            }
            catch (error) {
                if (!(error instanceof index_js_1.ExecutionError)) {
                    throw error;
                }
                t.is(error.attempts.length, 11, "A failed acquisition must have the configured number of retries.");
                for (const e of await Promise.allSettled(error.attempts)) {
                    t.is(e.status, "fulfilled");
                    if (e.status === "fulfilled") {
                        for (const v of (_b = (_a = e.value) === null || _a === void 0 ? void 0 : _a.votesAgainst) === null || _b === void 0 ? void 0 : _b.values()) {
                            t.is(v.message, "Connection is closed.");
                        }
                    }
                }
            }
        }
        catch (error) {
            fail(t, error);
        }
    });
    (0, ava_1.default)(`${namespace} - locks automatically expire`, async (t) => {
        try {
            const redlock = new index_js_1.default([redis]);
            const duration = 200;
            // Acquire a lock.
            const lock = await redlock.acquire(["{redlock}d"], duration);
            t.is(await redis.get("{redlock}d"), lock.value, "The lock value was incorrect.");
            // Wait until the lock expires.
            await new Promise((resolve) => setTimeout(resolve, 300, undefined));
            // Attempt to acquire another lock on the same resource.
            const lock2 = await redlock.acquire(["{redlock}d"], duration);
            t.is(await redis.get("{redlock}d"), lock2.value, "The lock value was incorrect.");
            // Release the lock.
            await lock2.release();
            t.is(await redis.get("{redlock}d"), null);
        }
        catch (error) {
            fail(t, error);
        }
    });
    (0, ava_1.default)(`${namespace} - individual locks are exclusive`, async (t) => {
        var _a, _b;
        try {
            const redlock = new index_js_1.default([redis]);
            const duration = Math.floor(Number.MAX_SAFE_INTEGER / 10);
            // Acquire a lock.
            const lock = await redlock.acquire(["{redlock}c"], duration);
            t.is(await redis.get("{redlock}c"), lock.value, "The lock value was incorrect.");
            t.is(Math.floor((await redis.pttl("{redlock}c")) / 200), Math.floor(duration / 200), "The lock expiration was off by more than 200ms");
            // Attempt to acquire another lock on the same resource.
            try {
                await redlock.acquire(["{redlock}c"], duration);
                throw new Error("This lock should not be acquired.");
            }
            catch (error) {
                if (!(error instanceof index_js_1.ExecutionError)) {
                    throw error;
                }
                t.is(error.attempts.length, 11, "A failed acquisition must have the configured number of retries.");
                for (const e of await Promise.allSettled(error.attempts)) {
                    t.is(e.status, "fulfilled");
                    if (e.status === "fulfilled") {
                        for (const v of (_b = (_a = e.value) === null || _a === void 0 ? void 0 : _a.votesAgainst) === null || _b === void 0 ? void 0 : _b.values()) {
                            t.assert(v instanceof index_js_1.ResourceLockedError, "The error must be a ResourceLockedError.");
                        }
                    }
                }
            }
            // Release the lock.
            await lock.release();
            t.is(await redis.get("{redlock}c"), null);
        }
        catch (error) {
            fail(t, error);
        }
    });
    (0, ava_1.default)(`${namespace} - overlapping multi-locks are exclusive`, async (t) => {
        var _a, _b;
        try {
            const redlock = new index_js_1.default([redis]);
            const duration = Math.floor(Number.MAX_SAFE_INTEGER / 10);
            // Acquire a lock.
            const lock = await redlock.acquire(["{redlock}c1", "{redlock}c2"], duration);
            t.is(await redis.get("{redlock}c1"), lock.value, "The lock value was incorrect.");
            t.is(await redis.get("{redlock}c2"), lock.value, "The lock value was incorrect.");
            t.is(Math.floor((await redis.pttl("{redlock}c1")) / 200), Math.floor(duration / 200), "The lock expiration was off by more than 200ms");
            t.is(Math.floor((await redis.pttl("{redlock}c2")) / 200), Math.floor(duration / 200), "The lock expiration was off by more than 200ms");
            // Attempt to acquire another lock with overlapping resources
            try {
                await redlock.acquire(["{redlock}c2", "{redlock}c3"], duration);
                throw new Error("This lock should not be acquired.");
            }
            catch (error) {
                if (!(error instanceof index_js_1.ExecutionError)) {
                    throw error;
                }
                t.is(await redis.get("{redlock}c1"), lock.value, "The original lock value must not be changed.");
                t.is(await redis.get("{redlock}c2"), lock.value, "The original lock value must not be changed.");
                t.is(await redis.get("{redlock}c3"), null, "The new resource must remain unlocked.");
                t.is(error.attempts.length, 11, "A failed acquisition must have the configured number of retries.");
                for (const e of await Promise.allSettled(error.attempts)) {
                    t.is(e.status, "fulfilled");
                    if (e.status === "fulfilled") {
                        for (const v of (_b = (_a = e.value) === null || _a === void 0 ? void 0 : _a.votesAgainst) === null || _b === void 0 ? void 0 : _b.values()) {
                            t.assert(v instanceof index_js_1.ResourceLockedError, "The error must be a ResourceLockedError.");
                        }
                    }
                }
            }
            // Release the lock.
            await lock.release();
            t.is(await redis.get("{redlock}c1"), null);
            t.is(await redis.get("{redlock}c2"), null);
            t.is(await redis.get("{redlock}c3"), null);
        }
        catch (error) {
            fail(t, error);
        }
    });
    (0, ava_1.default)(`${namespace} - the \`using\` helper acquires, extends, and releases locks`, async (t) => {
        try {
            const redlock = new index_js_1.default([redis]);
            const duration = 500;
            const valueP = redlock.using(["{redlock}x"], duration, {
                automaticExtensionThreshold: 200,
            }, async (signal) => {
                const lockValue = await redis.get("{redlock}x");
                t.assert(typeof lockValue === "string", "The lock value was not correctly acquired.");
                // Wait to ensure that the lock is extended
                await new Promise((resolve) => setTimeout(resolve, 700, undefined));
                t.is(signal.aborted, false, "The signal must not be aborted.");
                t.is(signal.error, undefined, "The signal must not have an error.");
                t.is(await redis.get("{redlock}x"), lockValue, "The lock value should not have changed.");
                return lockValue;
            });
            await valueP;
            t.is(await redis.get("{redlock}x"), null, "The lock was not released.");
        }
        catch (error) {
            fail(t, error);
        }
    });
    (0, ava_1.default)(`${namespace} - the \`using\` helper is exclusive`, async (t) => {
        try {
            const redlock = new index_js_1.default([redis]);
            const duration = 500;
            let locked = false;
            const [lock1, lock2] = await Promise.all([
                await redlock.using(["{redlock}y"], duration, {
                    automaticExtensionThreshold: 200,
                }, async (signal) => {
                    t.is(locked, false, "The resource must not already be locked.");
                    locked = true;
                    const lockValue = await redis.get("{redlock}y");
                    t.assert(typeof lockValue === "string", "The lock value was not correctly acquired.");
                    // Wait to ensure that the lock is extended
                    await new Promise((resolve) => setTimeout(resolve, 700, undefined));
                    t.is(signal.error, undefined, "The signal must not have an error.");
                    t.is(signal.aborted, false, "The signal must not be aborted.");
                    t.is(await redis.get("{redlock}y"), lockValue, "The lock value should not have changed.");
                    locked = false;
                    return lockValue;
                }),
                await redlock.using(["{redlock}y"], duration, {
                    automaticExtensionThreshold: 200,
                }, async (signal) => {
                    t.is(locked, false, "The resource must not already be locked.");
                    locked = true;
                    const lockValue = await redis.get("{redlock}y");
                    t.assert(typeof lockValue === "string", "The lock value was not correctly acquired.");
                    // Wait to ensure that the lock is extended
                    await new Promise((resolve) => setTimeout(resolve, 700, undefined));
                    t.is(signal.error, undefined, "The signal must not have an error.");
                    t.is(signal.aborted, false, "The signal must not be aborted.");
                    t.is(await redis.get("{redlock}y"), lockValue, "The lock value should not have changed.");
                    locked = false;
                    return lockValue;
                }),
            ]);
            t.not(lock1, lock2, "The locks must be different.");
            t.is(await redis.get("{redlock}y"), null, "The lock was not released.");
        }
        catch (error) {
            fail(t, error);
        }
    });
}
run("instance", new ioredis_1.default({ host: "redis-single-instance" }));
run("cluster", new ioredis_1.Cluster([{ host: "redis-single-cluster-1" }]));
//# sourceMappingURL=single.test.js.map