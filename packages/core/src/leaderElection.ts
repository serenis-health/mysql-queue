import { createScheduler } from "./scheduler";
import { Database } from "./database";
import { hostname } from "node:os";
import { Logger } from "./logger";

export function createLeaderElection(logger: Logger, database: Database, options: LeaderElectionOptions) {
  const { onBecomeLeader, onLoseLeadership, heartbeatIntervalMs, leaseDurationMs } = options;
  const heartbeatScheduler = createScheduler(maintainLeadership, logger, {
    intervalMs: heartbeatIntervalMs,
    runOnStart: true,
    taskName: "leader-election-heartbeat",
  });
  const instanceId = `${hostname()}:${process.pid}`;

  let isLeader = false;

  return {
    instanceId,
    isLeader() {
      return isLeader;
    },
    start() {
      heartbeatScheduler.start();
    },
    async stop() {
      heartbeatScheduler.stop();
      if (isLeader) {
        try {
          await database.releaseLeadership(instanceId);
          logger.info({ instanceId }, "leaderElection.releasedLeadership");
        } catch (error) {
          logger.error({ error, instanceId }, "leaderElection.releaseError");
        }
        isLeader = false;
      }
    },
  };

  async function tryBecomeLeader(): Promise<void> {
    try {
      const acquired = await database.tryAcquireLeadership(instanceId, leaseDurationMs);

      if (acquired && !isLeader) {
        isLeader = true;
        logger.info({ instanceId }, "leaderElection.becameLeader");
        onBecomeLeader?.();
      } else if (!acquired && isLeader) {
        isLeader = false;
        logger.warn({ instanceId }, "leaderElection.lostLeadership");
        onLoseLeadership?.();
      }
    } catch (error) {
      logger.error({ error, instanceId }, "leaderElection.tryAcquireError");
      if (isLeader) {
        isLeader = false;
        onLoseLeadership?.();
      }
    }
  }

  async function maintainLeadership(): Promise<void> {
    if (!isLeader) {
      await tryBecomeLeader();
      return;
    }

    try {
      const renewed = await database.renewLeadership(instanceId, leaseDurationMs);

      if (!renewed) {
        isLeader = false;
        logger.warn({ instanceId }, "leaderElection.failedToRenew");
        onLoseLeadership?.();
      } else {
        logger.trace({ instanceId }, "leaderElection.renewedLease");
      }
    } catch (error) {
      logger.error({ error, instanceId }, "leaderElection.renewError");
      isLeader = false;
      onLoseLeadership?.();
    }
  }
}

interface LeaderElectionOptions {
  leaseDurationMs: number;
  heartbeatIntervalMs: number;
  onBecomeLeader: () => void;
  onLoseLeadership: () => void;
}
