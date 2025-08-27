import Chance from "chance";
import { v4 as uuid } from "uuid";
import moment from "moment";

const chance = new Chance();

const createOrder = () => ({
  id: uuid(),
  first_name: chance.first(),
  last_name: chance.last(),
  ts: moment().format(),
});

export { createOrder };
