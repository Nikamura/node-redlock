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
function run(namespace, redisA, redisB, redisC) {
    ava_1.default.before(async () => {
        await Promise.all([
            redisA instanceof ioredis_1.Cluster && redisA.isCluster
                ? waitForCluster(redisA)
                : null,
            redisB instanceof ioredis_1.Cluster && redisB.isCluster
                ? waitForCluster(redisB)
                : null,
            redisC instanceof ioredis_1.Cluster && redisC.isCluster
                ? waitForCluster(redisC)
                : null,
        ]);
    });
    ava_1.default.before(async () => {
        await Promise.all([
            redisA.keys("*").then((keys) => ((keys === null || keys === void 0 ? void 0 : keys.length) ? redisA.del(keys) : null)),
            redisB.keys("*").then((keys) => ((keys === null || keys === void 0 ? void 0 : keys.length) ? redisB.del(keys) : null)),
            redisC.keys("*").then((keys) => ((keys === null || keys === void 0 ? void 0 : keys.length) ? redisC.del(keys) : null)),
        ]);
    });
    (0, ava_1.default)(`${namespace} - acquires, extends, and releases a single lock`, async (t) => {
        try {
            const redlock = new index_js_1.default([redisA, redisB, redisC]);
            const duration = Math.floor(Number.MAX_SAFE_INTEGER / 10);
            // Acquire a lock.
            let lock = await redlock.acquire(["{redlock}a"], duration);
            t.is(await redisA.get("{redlock}a"), lock.value, "The lock value was incorrect.");
            t.is(await redisB.get("{redlock}a"), lock.value, "The lock value was incorrect.");
            t.is(await redisC.get("{redlock}a"), lock.value, "The lock value was incorrect.");
            t.is(Math.floor((await redisA.pttl("{redlock}a")) / 200), Math.floor(duration / 200), "The lock expiration was off by more than 200ms");
            t.is(Math.floor((await redisB.pttl("{redlock}a")) / 200), Math.floor(duration / 200), "The lock expiration was off by more than 200ms");
            t.is(Math.floor((await redisC.pttl("{redlock}a")) / 200), Math.floor(duration / 200), "The lock expiration was off by more than 200ms");
            // Extend the lock.
            lock = await lock.extend(3 * duration);
            t.is(await redisA.get("{redlock}a"), lock.value, "The lock value was incorrect.");
            t.is(await redisB.get("{redlock}a"), lock.value, "The lock value was incorrect.");
            t.is(await redisC.get("{redlock}a"), lock.value, "The lock value was incorrect.");
            t.is(Math.floor((await redisA.pttl("{redlock}a")) / 200), Math.floor((3 * duration) / 200), "The lock expiration was off by more than 200ms");
            t.is(Math.floor((await redisB.pttl("{redlock}a")) / 200), Math.floor((3 * duration) / 200), "The lock expiration was off by more than 200ms");
            t.is(Math.floor((await redisC.pttl("{redlock}a")) / 200), Math.floor((3 * duration) / 200), "The lock expiration was off by more than 200ms");
            // Release the lock.
            await lock.release();
            t.is(await redisA.get("{redlock}a"), null);
            t.is(await redisB.get("{redlock}a"), null);
            t.is(await redisC.get("{redlock}a"), null);
        }
        catch (error) {
            fail(t, error);
        }
    });
    (0, ava_1.default)(`${namespace} - succeeds when a minority of clients fail`, async (t) => {
        try {
            const redlock = new index_js_1.default([redisA, redisB, redisC]);
            const duration = Math.floor(Number.MAX_SAFE_INTEGER / 10);
            // Set a value on redisC so that lock acquisition fails.
            await redisC.set("{redlock}b", "other");
            // Acquire a lock.
            let lock = await redlock.acquire(["{redlock}b"], duration);
            t.is(await redisA.get("{redlock}b"), lock.value, "The lock value was incorrect.");
            t.is(await redisB.get("{redlock}b"), lock.value, "The lock value was incorrect.");
            t.is(await redisC.get("{redlock}b"), "other", "The lock value was changed.");
            t.is(Math.floor((await redisA.pttl("{redlock}b")) / 200), Math.floor(duration / 200), "The lock expiration was off by more than 200ms");
            t.is(Math.floor((await redisB.pttl("{redlock}b")) / 200), Math.floor(duration / 200), "The lock expiration was off by more than 200ms");
            t.is(await redisC.pttl("{redlock}b"), -1, "The lock expiration was changed");
            // Extend the lock.
            lock = await lock.extend(3 * duration);
            t.is(await redisA.get("{redlock}b"), lock.value, "The lock value was incorrect.");
            t.is(await redisB.get("{redlock}b"), lock.value, "The lock value was incorrect.");
            t.is(await redisC.get("{redlock}b"), "other", "The lock value was changed.");
            t.is(Math.floor((await redisA.pttl("{redlock}b")) / 200), Math.floor((3 * duration) / 200), "The lock expiration was off by more than 200ms");
            t.is(Math.floor((await redisB.pttl("{redlock}b")) / 200), Math.floor((3 * duration) / 200), "The lock expiration was off by more than 200ms");
            t.is(await redisC.pttl("{redlock}b"), -1, "The lock expiration was changed");
            // Release the lock.
            await lock.release();
            t.is(await redisA.get("{redlock}b"), null);
            t.is(await redisB.get("{redlock}b"), null);
            t.is(await redisC.get("{redlock}b"), "other");
            await redisC.del("{redlock}b");
        }
        catch (error) {
            fail(t, error);
        }
    });
    (0, ava_1.default)(`${namespace} - fails when a majority of clients fail`, async (t) => {
        var _a, _b;
        try {
            const redlock = new index_js_1.default([redisA, redisB, redisC]);
            const duration = Math.floor(Number.MAX_SAFE_INTEGER / 10);
            // Set a value on redisB and redisC so that lock acquisition fails.
            await redisB.set("{redlock}c", "other1");
            await redisC.set("{redlock}c", "other2");
            // Acquire a lock.
            try {
                await redlock.acquire(["{redlock}c"], duration);
                throw new Error("This lock should not be acquired.");
            }
            catch (error) {
                if (!(error instanceof index_js_1.ExecutionError)) {
                    throw error;
                }
                t.is(error.attempts.length, 11, "A failed acquisition must have the configured number of retries.");
                t.is(await redisA.get("{redlock}c"), null);
                t.is(await redisB.get("{redlock}c"), "other1");
                t.is(await redisC.get("{redlock}c"), "other2");
                for (const e of await Promise.allSettled(error.attempts)) {
                    t.is(e.status, "fulfilled");
                    if (e.status === "fulfilled") {
                        for (const v of (_b = (_a = e.value) === null || _a === void 0 ? void 0 : _a.votesAgainst) === null || _b === void 0 ? void 0 : _b.values()) {
                            t.assert(v instanceof index_js_1.ResourceLockedError, "The error was of the wrong type.");
                            t.is(v.message, "The operation was applied to: 0 of the 1 requested resources.");
                        }
                    }
                }
            }
            await redisB.del("{redlock}c");
            await redisC.del("{redlock}c");
        }
        catch (error) {
            fail(t, error);
        }
    });
}
run("instance", new ioredis_1.default({ host: "redis-multi-instance-a" }), new ioredis_1.default({ host: "redis-multi-instance-b" }), new ioredis_1.default({ host: "redis-multi-instance-c" }));
run("cluster", new ioredis_1.Cluster([{ host: "redis-multi-cluster-a-1" }]), new ioredis_1.Cluster([{ host: "redis-multi-cluster-b-1" }]), new ioredis_1.Cluster([{ host: "redis-multi-cluster-c-1" }]));
//# sourceMappingURL=multi.test.js.map