"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable @typescript-eslint/no-explicit-any */
const bull_1 = __importDefault(require("bull"));
const mongodb_1 = require("mongodb");
exports.createQueues = (queues, { dbName = "scraper", mongo, redisUrl }) => {
    let client;
    const info = Object.fromEntries(Object.entries(queues).map(([tag, value_]) => {
        const queue = (redisUrl === undefined
            ? new bull_1.default(tag)
            : new bull_1.default(tag, redisUrl));
        const value = value_;
        queue.save = async (...data) => {
            var _a;
            if (!client) {
                client = await mongodb_1.MongoClient.connect(mongo.url, {
                    useNewUrlParser: true,
                    ...mongo,
                });
            }
            const collection = client
                .db(dbName)
                .collection((_a = value.storageKey) !== null && _a !== void 0 ? _a : tag);
            await collection.bulkWrite(data.map(($set) => ({
                updateOne: {
                    filter: { _id: $set.id },
                    update: { $set },
                    upsert: true,
                },
            })));
        };
        queue.push = async (data, options) => {
            var _a;
            return await queue.add(tag, { ...data, tag }, { ...((_a = value.options) !== null && _a !== void 0 ? _a : {}), ...(options !== null && options !== void 0 ? options : {}) });
        };
        if ("callback" in value) {
            queue.start = async () => {
                var _a;
                return await queue.process(tag, (_a = value.concurrency) !== null && _a !== void 0 ? _a : 1, value.callback);
            };
            return [tag, queue];
        }
        else if ("processor" in value) {
            queue.start = async () => {
                var _a;
                return await queue.process(tag, (_a = value.concurrency) !== null && _a !== void 0 ? _a : 1, value.processor);
            };
            return [tag, queue];
        }
        else {
            throw new Error(`Queue definition must either have a callback or a processor`);
        }
    }));
    const start = async (...queues) => {
        await Promise.all(Object.entries(info)
            .filter(([name]) => queues.length === 0 || queues.includes(name))
            .map(async ([, queue]) => {
            await queue.start();
        }));
    };
    return [info, start];
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvaW5kZXgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSx1REFBdUQ7QUFDdkQsZ0RBS2E7QUFDYixxQ0FBdUQ7QUErQjFDLFFBQUEsWUFBWSxHQUFHLENBQ3hCLE1BQStDLEVBQy9DLEVBQUMsTUFBTSxHQUFHLFNBQVMsRUFBRSxLQUFLLEVBQUUsUUFBUSxFQUFxQixFQUN0QyxFQUFFO0lBQ3JCLElBQUksTUFBK0IsQ0FBQTtJQUVuQyxNQUFNLElBQUksR0FBSSxNQUFNLENBQUMsV0FBVyxDQUM1QixNQUFNLENBQUMsT0FBTyxDQUFDLE1BQU0sQ0FBQyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsR0FBRyxFQUFFLE1BQU0sQ0FBQyxFQUFFLEVBQUU7UUFDekMsTUFBTSxLQUFLLEdBQUksQ0FBQyxRQUFRLEtBQUssU0FBUztZQUNsQyxDQUFDLENBQUMsSUFBSSxjQUFnQixDQUFDLEdBQUcsQ0FBQztZQUMzQixDQUFDLENBQUMsSUFBSSxjQUFnQixDQUNoQixHQUFHLEVBQ0gsUUFBUSxDQUNYLENBQWtDLENBQUE7UUFFekMsTUFBTSxLQUFLLEdBQUcsTUFBOEIsQ0FBQTtRQUU1QyxLQUFLLENBQUMsSUFBSSxHQUFHLEtBQUssRUFBRSxHQUFHLElBQUksRUFBRSxFQUFFOztZQUMzQixJQUFJLENBQUMsTUFBTSxFQUFFO2dCQUNULE1BQU0sR0FBRyxNQUFNLHFCQUFXLENBQUMsT0FBTyxDQUFDLEtBQUssQ0FBQyxHQUFHLEVBQUU7b0JBQzFDLGVBQWUsRUFBRSxJQUFJO29CQUNyQixHQUFHLEtBQUs7aUJBQ1gsQ0FBQyxDQUFBO2FBQ0w7WUFDRCxNQUFNLFVBQVUsR0FBRyxNQUFNO2lCQUNwQixFQUFFLENBQUMsTUFBTSxDQUFDO2lCQUNWLFVBQVUsT0FBQyxLQUFLLENBQUMsVUFBVSxtQ0FBSSxHQUFHLENBQUMsQ0FBQTtZQUN4QyxNQUFNLFVBQVUsQ0FBQyxTQUFTLENBQ3RCLElBQUksQ0FBQyxHQUFHLENBQUMsQ0FBQyxJQUFJLEVBQUUsRUFBRSxDQUFDLENBQUM7Z0JBQ2hCLFNBQVMsRUFBRTtvQkFDUCxNQUFNLEVBQUUsRUFBQyxHQUFHLEVBQUUsSUFBSSxDQUFDLEVBQUUsRUFBQztvQkFDdEIsTUFBTSxFQUFFLEVBQUMsSUFBSSxFQUFDO29CQUNkLE1BQU0sRUFBRSxJQUFJO2lCQUNmO2FBQ0osQ0FBQyxDQUFDLENBQ04sQ0FBQTtRQUNMLENBQUMsQ0FBQTtRQUNELEtBQUssQ0FBQyxJQUFJLEdBQUcsS0FBSyxFQUFFLElBQUksRUFBRSxPQUFPLEVBQUUsRUFBRTs7WUFDakMsT0FBQSxNQUFNLEtBQUssQ0FBQyxHQUFHLENBQ1gsR0FBRyxFQUNILEVBQUMsR0FBRyxJQUFJLEVBQUUsR0FBRyxFQUFDLEVBQ2QsRUFBQyxHQUFHLE9BQUMsS0FBSyxDQUFDLE9BQU8sbUNBQUksRUFBRSxDQUFDLEVBQUUsR0FBRyxDQUFDLE9BQU8sYUFBUCxPQUFPLGNBQVAsT0FBTyxHQUFJLEVBQUUsQ0FBQyxFQUFDLENBQ2pELENBQUE7U0FBQSxDQUFBO1FBRUwsSUFBSSxVQUFVLElBQUksS0FBSyxFQUFFO1lBQ3JCLEtBQUssQ0FBQyxLQUFLLEdBQUcsS0FBSyxJQUFJLEVBQUU7O2dCQUNyQixPQUFBLE1BQU0sS0FBSyxDQUFDLE9BQU8sQ0FDZixHQUFHLFFBQ0gsS0FBSyxDQUFDLFdBQVcsbUNBQUksQ0FBQyxFQUN0QixLQUFLLENBQUMsUUFBUSxDQUNqQixDQUFBO2FBQUEsQ0FBQTtZQUNMLE9BQU8sQ0FBQyxHQUFHLEVBQUUsS0FBSyxDQUFVLENBQUE7U0FDL0I7YUFBTSxJQUFJLFdBQVcsSUFBSSxLQUFLLEVBQUU7WUFDN0IsS0FBSyxDQUFDLEtBQUssR0FBRyxLQUFLLElBQUksRUFBRTs7Z0JBQ3JCLE9BQUEsTUFBTSxLQUFLLENBQUMsT0FBTyxDQUNmLEdBQUcsUUFDSCxLQUFLLENBQUMsV0FBVyxtQ0FBSSxDQUFDLEVBQ3RCLEtBQUssQ0FBQyxTQUFTLENBQ2xCLENBQUE7YUFBQSxDQUFBO1lBQ0wsT0FBTyxDQUFDLEdBQUcsRUFBRSxLQUFLLENBQVUsQ0FBQTtTQUMvQjthQUFNO1lBQ0gsTUFBTSxJQUFJLEtBQUssQ0FDWCw2REFBNkQsQ0FDaEUsQ0FBQTtTQUNKO0lBQ0wsQ0FBQyxDQUFDLENBR0wsQ0FBQTtJQUVELE1BQU0sS0FBSyxHQUFHLEtBQUssRUFBRSxHQUFHLE1BQW1CLEVBQUUsRUFBRTtRQUMzQyxNQUFNLE9BQU8sQ0FBQyxHQUFHLENBQ2IsTUFBTSxDQUFDLE9BQU8sQ0FBQyxJQUFJLENBQUM7YUFDZixNQUFNLENBQ0gsQ0FBQyxDQUFDLElBQUksQ0FBQyxFQUFFLEVBQUUsQ0FBQyxNQUFNLENBQUMsTUFBTSxLQUFLLENBQUMsSUFBSSxNQUFNLENBQUMsUUFBUSxDQUFDLElBQUksQ0FBQyxDQUMzRDthQUNBLEdBQUcsQ0FBQyxLQUFLLEVBQUUsQ0FBQyxFQUFFLEtBQUssQ0FBQyxFQUFFLEVBQUU7WUFDckIsTUFBTSxLQUFLLENBQUMsS0FBSyxFQUFFLENBQUE7UUFDdkIsQ0FBQyxDQUFDLENBQ1QsQ0FBQTtJQUNMLENBQUMsQ0FBQTtJQUVELE9BQU8sQ0FBQyxJQUFJLEVBQUUsS0FBSyxDQUFVLENBQUE7QUFDakMsQ0FBQyxDQUFBIn0=