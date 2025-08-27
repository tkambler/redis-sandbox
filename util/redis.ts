import delay from "delay";
import Redis from "ioredis";
import { log } from "@util/log";

const createStreamHelper = (redis: Redis) => {
  const createGroup = async ({
    stream,
    group,
  }: {
    stream: string;
    group: string;
  }) => {
    try {
      await redis.xgroup("CREATE", stream, group, "$", "MKSTREAM");
    } catch (err) {
      if (!String(err?.message ?? "").includes("BUSYGROUP")) {
        throw err;
      }
    }
  };

  const publishEvent = ({ data, stream }) => {
    return redis.xadd(stream, "*", "data", JSON.stringify(data));
  };

  const startConsumer = async ({ stream, group, consumer }, fn) => {
    while (true) {
      try {
        const res: any[] = await redis.xreadgroup(
          "GROUP",
          group,
          consumer,
          "COUNT",
          10,
          "BLOCK",
          5000,
          "STREAMS",
          stream,
          ">"
        );

        if (!res) {
          continue;
        }

        for (const [_stream, messages] of res) {
          for (const [id, fields] of messages) {
            const data = JSON.parse(fields[1]);
            if (!data) {
              await redis.xack(stream, group, id);
              continue;
            }
            await fn({
              id,
              data,
            });
            await redis.xack(stream, group, id);
            continue;
          }
        }
      } catch (err) {
        log.error(
          { error: err?.message ?? "-", group, consumer },
          "Consumer read error"
        );
        await delay(1000);
      }
    }
  };

  return {
    createGroup,
    publishEvent,
    startConsumer,
  };
};

export { createStreamHelper };
