import {
  logger,
  QueueName,
  recordHistogram,
} from "@langfuse/shared/src/server";
import { getQueue } from "@langfuse/shared/src/server";
import { env } from "../../env";

export class DlqRetryService {
  private static retryQueues = [
    QueueName.ProjectDelete,
    QueueName.TraceDelete,
    QueueName.ScoreDelete,
    QueueName.BatchActionQueue,
    QueueName.DataRetentionProcessingQueue,
  ];

  // called each 10 minutes, defined by the bull cron job
  public static async retryDeadLetterQueue() {
    logger.info(
      `Retrying dead letter queues for queues: ${DlqRetryService.retryQueues.join(
        ", ",
      )}`,
    );
    const retryQueues = DlqRetryService.retryQueues;
    for (const queueName of retryQueues) {
      const queue = getQueue(queueName as QueueName);

      if (!queue) {
        logger.error(`Queue ${queueName} not found`);
        continue;
      }

      // Find failed jobs
      const failedJobs = await queue.getFailed();
      logger.info(
        `Found ${failedJobs.length} failed jobs in queue ${queueName}`,
      );

      const now = Date.now();
      const maxRetryAgeMs =
        env.LANGFUSE_DLQ_MAX_RETRY_AGE_HOURS * 60 * 60 * 1000;
      const retryDelayMs = env.LANGFUSE_DLQ_RETRY_DELAY_HOURS * 60 * 60 * 1000;

      for (const job of failedJobs) {
        try {
          const projectId = job.data.payload.projectId;
          const jobTimestamp = job.timestamp;
          const jobAge = now - jobTimestamp;
          const retryCount = job.attemptsMade || 0;

          // Skip if job is too old or has too many retries
          if (jobAge > maxRetryAgeMs) {
            logger.info(
              `Job ${job.id} in queue ${queueName} is too old (${jobAge}ms), skipping retry`,
              { projectId, queueName, jobAge, retryCount },
            );
            continue;
          }

          if (retryCount >= env.LANGFUSE_DLQ_MAX_RETRIES) {
            logger.info(
              `Job ${job.id} in queue ${queueName} has too many retries (${retryCount}), skipping`,
              { projectId, queueName, jobAge, retryCount },
            );
            continue;
          }

          // Calculate delay since last attempt
          const lastAttemptTime = job.finishedOn || jobTimestamp;
          const timeSinceLastAttempt = now - lastAttemptTime;

          // Only retry if enough time has passed since last attempt
          if (timeSinceLastAttempt < retryDelayMs) {
            logger.debug(
              `Job ${job.id} in queue ${queueName} not ready for retry yet (${timeSinceLastAttempt}ms since last attempt)`,
              { projectId, queueName, timeSinceLastAttempt, retryCount },
            );
            continue;
          }

          recordHistogram("langfuse.dlq_retry_delay", timeSinceLastAttempt, {
            unit: "milliseconds",
            projectId,
            queueName,
          });

          await job.retry();
          logger.info(`Retried job ${job.id} in queue ${queueName}`, {
            projectId,
            queueName,
            jobAge,
            retryCount: retryCount + 1,
          });
        } catch (error) {
          logger.error(
            `Failed to retry job ${job.id} in queue ${queueName}:`,
            error,
          );
        }
      }
    }
  }
}
