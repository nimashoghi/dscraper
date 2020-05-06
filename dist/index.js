"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable @typescript-eslint/no-explicit-any */
const bull_1 = __importDefault(require("bull"));
const mongodb_1 = require("mongodb");
exports.createQueues = (queues, { dbName = "scraper", mongo, redis }) => {
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
            return await queue.add(tag, { ...data, tag }, { ...((_a = value.options) !== null && _a !== void 0 ? _a : {}), ...(options !== null && options !== void 0 ? options : {}) });
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvaW5kZXgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSx1REFBdUQ7QUFDdkQsZ0RBQW9FO0FBQ3BFLHFDQUF1RDtBQW9DMUMsUUFBQSxZQUFZLEdBQUcsQ0FDeEIsTUFBK0MsRUFDL0MsRUFBQyxNQUFNLEdBQUcsU0FBUyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQXFCLEVBQ25DLEVBQUU7SUFDckIsSUFBSSxNQUErQixDQUFBO0lBRW5DLE1BQU0sT0FBTyxHQUFHLE1BQU0sQ0FBQyxPQUFPLENBQUMsTUFBTSxDQUFDLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxHQUFHLEVBQUUsTUFBTSxDQUFDLEVBQUUsRUFBRTtRQUN6RCxNQUFNLEtBQUssR0FBRyxJQUFJLGNBQUksQ0FBQyxHQUFHLEVBQUUsS0FBSyxDQUFDLEdBQUcsQ0FBQyxDQUFBO1FBRXRDLE1BQU0sS0FBSyxHQUFHLE1BQThCLENBQUE7UUFFNUMsS0FBSyxDQUFDLElBQUksR0FBRyxLQUFLLEVBQUUsR0FBRyxJQUFJLEVBQUUsRUFBRTs7WUFDM0IsSUFBSSxDQUFDLE1BQU0sRUFBRTtnQkFDVCxNQUFNLEdBQUcsTUFBTSxxQkFBVyxDQUFDLE9BQU8sQ0FBQyxLQUFLLENBQUMsR0FBRyxFQUFFO29CQUMxQyxlQUFlLEVBQUUsSUFBSTtvQkFDckIsR0FBRyxLQUFLO2lCQUNYLENBQUMsQ0FBQTthQUNMO1lBQ0QsSUFBSSxJQUFJLENBQUMsTUFBTSxLQUFLLENBQUMsRUFBRTtnQkFDbkIsT0FBTTthQUNUO1lBRUQsTUFBTSxVQUFVLEdBQUcsTUFBTTtpQkFDcEIsRUFBRSxDQUFDLE1BQU0sQ0FBQztpQkFDVixVQUFVLE9BQUMsS0FBSyxDQUFDLFVBQVUsbUNBQUksR0FBRyxDQUFDLENBQUE7WUFDeEMsTUFBTSxVQUFVLENBQUMsU0FBUyxDQUN0QixJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsSUFBSSxF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