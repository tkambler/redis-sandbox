import Pino from "pino";
import delay from "delay";
import Redis from "ioredis";
import Chance from "chance";
import { v4 as uuid } from "uuid";
import moment from "moment";

const redis = new Redis({
  host: "127.0.0.1",
  port: 6324,
});

const chance = new Chance();

const log = Pino();

const stream = "orders";

const groups = [
  {
    name: "group1",
    consumers: ["fooConsumer", "barConsumer"],
  },
  {
    name: "group2",
    consumers: ["herpConsumer", "derpConsumer"],
  },
];

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

const createOrder = () => ({
  id: uuid(),
  first_name: chance.first(),
  last_name: chance.last(),
  ts: moment().format(),
});

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

export {
  redis,
  chance,
  log,
  stream,
  groups,
  createGroup,
  createOrder,
  publishEvent,
  startConsumer,
};
