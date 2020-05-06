"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable @typescript-eslint/no-explicit-any */
const bull_1 = __importDefault(require("bull"));
const mongodb_1 = require("mongodb");
exports.createQueues = (queues, { dbName = "scraper", defaultOptions, mongo, redis }) => {
    let client;
    const entries = Object.entries(queues).map(([tag, value_]) => {
        const queue = new bull_1.default(tag, redis.url);
        const value = value_;
        queue.save = async (...data) => {
            var _a;
            if (!client) {
                client = await mongodb_1.MongoClient.connect(mongo.url, {
                    useNewUrlParser: true,
                    ...mongo,
                });
            }
            if (data.length === 0) {
                return;
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
            return await queue.add(tag, { ...data, tag }, {
                ...(defaultOptions !== null && defaultOptions !== void 0 ? defaultOptions : {}),
                ...((_a = value.options) !== null && _a !== void 0 ? _a : {}),
                ...(options !== null && options !== void 0 ? options : {}),
            });
        };
        if ("callback" in value) {
            queue.init = async () => {
                if (value.concurrency === undefined) {
                    await queue.process(tag, value.callback);
                }
                else {
                    await queue.process(tag, value.concurrency, value.callback);
                }
            };
            return [tag, queue];
        }
        else if ("processor" in value) {
            queue.init = async () => {
                if (value.concurrency === undefined) {
                    await queue.process(tag, require.resolve(value.processor));
                }
                else {
                    await queue.process(tag, value.concurrency, require.resolve(value.processor));
                }
            };
            return [tag, queue];
        }
        else {
            throw new Error(`Queue definition must either have a callback or a processor`);
        }
    });
    const info = Object.fromEntries(entries);
    const start = async (...queues) => {
        const queues_ = new Set(queues.length === 0 ? Object.keys(info) : queues);
        await Promise.all(entries
            .filter(([name]) => queues_.has(name))
            .map(async ([, queue]) => {
            await queue.init();
        }));
    };
    return [info, start];
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvaW5kZXgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSx1REFBdUQ7QUFDdkQsZ0RBQW9FO0FBQ3BFLHFDQUF1RDtBQXFDMUMsUUFBQSxZQUFZLEdBQUcsQ0FDeEIsTUFBK0MsRUFDL0MsRUFBQyxNQUFNLEdBQUcsU0FBUyxFQUFFLGNBQWMsRUFBRSxLQUFLLEVBQUUsS0FBSyxFQUFxQixFQUNuRCxFQUFFO0lBQ3JCLElBQUksTUFBK0IsQ0FBQTtJQUVuQyxNQUFNLE9BQU8sR0FBRyxNQUFNLENBQUMsT0FBTyxDQUFDLE1BQU0sQ0FBQyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsR0FBRyxFQUFFLE1BQU0sQ0FBQyxFQUFFLEVBQUU7UUFDekQsTUFBTSxLQUFLLEdBQUcsSUFBSSxjQUFJLENBQUMsR0FBRyxFQUFFLEtBQUssQ0FBQyxHQUFHLENBQUMsQ0FBQTtRQUV0QyxNQUFNLEtBQUssR0FBRyxNQUE4QixDQUFBO1FBRTVDLEtBQUssQ0FBQyxJQUFJLEdBQUcsS0FBSyxFQUFFLEdBQUcsSUFBSSxFQUFFLEVBQUU7O1lBQzNCLElBQUksQ0FBQyxNQUFNLEVBQUU7Z0JBQ1QsTUFBTSxHQUFHLE1BQU0scUJBQVcsQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLEdBQUcsRUFBRTtvQkFDMUMsZUFBZSxFQUFFLElBQUk7b0JBQ3JCLEdBQUcsS0FBSztpQkFDWCxDQUFDLENBQUE7YUFDTDtZQUNELElBQUksSUFBSSxDQUFDLE1BQU0sS0FBSyxDQUFDLEVBQUU7Z0JBQ25CLE9BQU07YUFDVDtZQUVELE1BQU0sVUFBVSxHQUFHLE1BQU07aUJBQ3BCLEVBQUUsQ0FBQyxNQUFNLENBQUM7aUJBQ1YsVUFBVSxPQUFDLEtBQUssQ0FBQyxVQUFVLG1DQUFJLEdBQUcsQ0FBQyxDQUFBO1lBQ3hDLE1BQU0sVUFBVSxDQUFDLFNBQVMsQ0FDdEIsSUFBSSxDQUFDLEdBQUcsQ0FBQyxDQUFDLElBQUksRUFBRSxFQUFFLENBQUMsQ0FBQztnQkFDaEIsU0FBUyxFQUFFO29CQUNQLE1BQU0sRUFBRSxFQUFDLEdBQUcsRUFBRSxJQUFJLENBQUMsRUFBRSxFQUFDO29CQUN0QixNQUFNLEVBQUUsRUFBQyxJQUFJLEVBQUM7b0JBQ2QsTUFBTSxFQUFFLElBQUk7aUJBQ2Y7YUFDSixDQUFDLENBQUMsQ0FDTixDQUFBO1FBQ0wsQ0FBQyxDQUFBO1FBQ0QsS0FBSyxDQUFDLElBQUksR0FBRyxLQUFLLEVBQUUsSUFBSSxFQUFFLE9BQU8sRUFBRSxFQUFFOztZQUNqQyxPQUFBLE1BQU0sS0FBSyxDQUFDLEdBQUcsQ0FDWCxHQUFHLEVBQ0gsRUFBQyxHQUFHLElBQUksRUFBRSxHQUFHLEVBQUMsRUFDZDtnQkFDSSxHQUFHLENBQUMsY0FBYyxhQUFkLGNBQWMsY0FBZCxjQUFjLEdBQUksRUFBRSxDQUFDO2dCQUN6QixHQUFHLE9BQUMsS0FBSyxDQUFDLE9BQU8sbUNBQUksRUFBRSxDQUFDO2dCQUN4QixHQUFHLENBQUMsT0FBTyxhQUFQLE9BQU8sY0FBUCxPQUFPLEdBQUksRUFBRSxDQUFDO2FBQ3JCLENBQ0osQ0FBQTtTQUFBLENBQUE7UUFFTCxJQUFJLFVBQVUsSUFBSSxLQUFLLEVBQUU7WUFDckIsS0FBSyxDQUFDLElBQUksR0FBRyxLQUFLLElBQUksRUFBRTtnQkFDcEIsSUFBSSxLQUFLLENBQUMsV0FBVyxLQUFLLFNBQVMsRUFBRTtvQkFDakMsTUFBTSxLQUFLLENBQUMsT0FBTyxDQUFDLEdBQUcsRUFBRSxLQUFLLENBQUMsUUFBUSxDQUFDLENBQUE7aUJBQzNDO3FCQUFNO29CQUNILE1BQU0sS0FBSyxDQUFDLE9BQU8sQ0FBQyxHQUFHLEVBQUUsS0FBSyxDQUFDLFdBQVcsRUFBRSxLQUFLLENBQUMsUUFBUSxDQUFDLENBQUE7aUJBQzlEO1lBQ0wsQ0FBQyxDQUFBO1lBQ0QsT0FBTyxDQUFDLEdBQUcsRUFBRSxLQUFLLENBQVUsQ0FBQTtTQUMvQjthQUFNLElBQUksV0FBVyxJQUFJLEtBQUssRUFBRTtZQUM3QixLQUFLLENBQUMsSUFBSSxHQUFHLEtBQUssSUFBSSxFQUFFO2dCQUNwQixJQUFJLEtBQUssQ0FBQyxXQUFXLEtBQUssU0FBUyxFQUFFO29CQUNqQyxNQUFNLEtBQUssQ0FBQyxPQUFPLENBQUMsR0FBRyxFQUFFLE9BQU8sQ0FBQyxPQUFPLENBQUMsS0FBSyxDQUFDLFNBQVMsQ0FBQyxDQUFDLENBQUE7aUJBQzdEO3FCQUFNO29CQUNILE1BQU0sS0FBSyxDQUFDLE9BQU8sQ0FDZixHQUFHLEVBQ0gsS0FBSyxDQUFDLFdBQVcsRUFDakIsT0FBTyxDQUFDLE9BQU8sQ0FBQyxLQUFLLENBQUMsU0FBUyxDQUFDLENBQ25DLENBQUE7aUJBQ0o7WUFDTCxDQUFDLENBQUE7WUFDRCxPQUFPLENBQUMsR0FBRyxFQUFFLEtBQUssQ0FBVSxDQUFBO1NBQy9CO2FBQU07WUFDSCxNQUFNLElBQUksS0FBSyxDQUNYLDZEQUE2RCxDQUNoRSxDQUFBO1NBQ0o7SUFDTCxDQUFDLENBQUMsQ0FBQTtJQUNGLE1BQU0sSUFBSSxHQUFJLE1BQU0sQ0FBQyxXQUFXLENBQUMsT0FBTyxDQUV2QyxDQUFBO0lBRUQsTUFBTSxLQUFLLEdBQUcsS0FBSyxFQUFFLEdBQUcsTUFBbUIsRUFBRSxFQUFFO1FBQzNDLE1BQU0sT0FBTyxHQUFHLElBQUksR0FBRyxDQUNuQixNQUFNLENBQUMsTUFBTSxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQUUsTUFBTSxDQUFDLElBQUksQ0FBQyxJQUFJLENBQWlCLENBQUMsQ0FBQyxDQUFDLE1BQU0sQ0FDcEUsQ0FBQTtRQUNELE1BQU0sT0FBTyxDQUFDLEdBQUcsQ0FDYixPQUFPO2FBQ0YsTUFBTSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsRUFBRSxFQUFFLENBQUMsT0FBTyxDQUFDLEdBQUcsQ0FBQyxJQUFJLENBQUMsQ0FBQzthQUNyQyxHQUFHLENBQUMsS0FBSyxFQUFFLENBQUMsRUFBRSxLQUFLLENBQUMsRUFBRSxFQUFFO1lBQ3JCLE1BQU0sS0FBSyxDQUFDLElBQUksRUFBRSxDQUFBO1FBQ3RCLENBQUMsQ0FBQyxDQUNULENBQUE7SUFDTCxDQUFDLENBQUE7SUFFRCxPQUFPLENBQUMsSUFBSSxFQUFFLEtBQUssQ0FBVSxDQUFBO0FBQ2pDLENBQUMsQ0FBQSJ9