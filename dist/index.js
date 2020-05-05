"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
/* eslint-disable @typescript-eslint/no-explicit-any */
const bull_1 = __importDefault(require("bull"));
const mongodb_1 = require("mongodb");
exports.createQueues = (queues, { dbName = "scraper", mongo, redisUrl } = {}) => {
    let client;
    const info = Object.fromEntries(Object.entries(queues).map(([tag, value_]) => {
        const queue = (redisUrl === undefined
            ? new bull_1.default(tag)
            : new bull_1.default(tag, redisUrl));
        const value = value_;
        queue.save = async (...data) => {
            var _a;
            if (!client) {
                client = await mongodb_1.MongoClient.connect("", mongo);
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
        queue.push = async (data, options) => await queue.add(tag, { ...data, tag }, options);
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvaW5kZXgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSx1REFBdUQ7QUFDdkQsZ0RBS2E7QUFDYixxQ0FBdUQ7QUE4QjFDLFFBQUEsWUFBWSxHQUFHLENBQ3hCLE1BQStDLEVBQy9DLEVBQUMsTUFBTSxHQUFHLFNBQVMsRUFBRSxLQUFLLEVBQUUsUUFBUSxLQUF3QixFQUFFLEVBQzNDLEVBQUU7SUFDckIsSUFBSSxNQUErQixDQUFBO0lBRW5DLE1BQU0sSUFBSSxHQUFJLE1BQU0sQ0FBQyxXQUFXLENBQzVCLE1BQU0sQ0FBQyxPQUFPLENBQUMsTUFBTSxDQUFDLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxHQUFHLEVBQUUsTUFBTSxDQUFDLEVBQUUsRUFBRTtRQUN6QyxNQUFNLEtBQUssR0FBSSxDQUFDLFFBQVEsS0FBSyxTQUFTO1lBQ2xDLENBQUMsQ0FBQyxJQUFJLGNBQWdCLENBQUMsR0FBRyxDQUFDO1lBQzNCLENBQUMsQ0FBQyxJQUFJLGNBQWdCLENBQ2hCLEdBQUcsRUFDSCxRQUFRLENBQ1gsQ0FBa0MsQ0FBQTtRQUV6QyxNQUFNLEtBQUssR0FBRyxNQUE4QixDQUFBO1FBRTVDLEtBQUssQ0FBQyxJQUFJLEdBQUcsS0FBSyxFQUFFLEdBQUcsSUFBSSxFQUFFLEVBQUU7O1lBQzNCLElBQUksQ0FBQyxNQUFNLEVBQUU7Z0JBQ1QsTUFBTSxHQUFHLE1BQU0scUJBQVcsQ0FBQyxPQUFPLENBQUMsRUFBRSxFQUFFLEtBQUssQ0FBQyxDQUFBO2FBQ2hEO1lBQ0QsTUFBTSxVQUFVLEdBQUcsTUFBTTtpQkFDcEIsRUFBRSxDQUFDLE1BQU0sQ0FBQztpQkFDVixVQUFVLE9BQUMsS0FBSyxDQUFDLFVBQVUsbUNBQUksR0FBRyxDQUFDLENBQUE7WUFDeEMsTUFBTSxVQUFVLENBQUMsU0FBUyxDQUN0QixJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsSUFBSSxFQUFFLEVBQUUsQ0FBQyxDQUFDO2dCQUNoQixTQUFTLEVBQUU7b0JBQ1AsTUFBTSxFQUFFLEVBQUMsR0FBRyxFQUFFLElBQUksQ0FBQyxFQUFFLEVBQUM7b0JBQ3RCLE1BQU0sRUFBRSxFQUFDLElBQUksRUFBQztvQkFDZCxNQUFNLEVBQUUsSUFBSTtpQkFDZjthQUNKLENBQUMsQ0FBQyxDQUNOLENBQUE7UUFDTCxDQUFDLENBQUE7UUFDRCxLQUFLLENBQUMsSUFBSSxHQUFHLEtBQUssRUFBRSxJQUFJLEVBQUUsT0FBTyxFQUFFLEVBQUUsQ0FDakMsTUFBTSxLQUFLLENBQUMsR0FBRyxDQUFDLEdBQUcsRUFBRSxFQUFDLEdBQUcsSUFBSSxFQUFFLEdBQUcsRUFBQyxFQUFFLE9BQU8sQ0FBQyxDQUFBO1FBRWpELElBQUksVUFBVSxJQUFJLEtBQUssRUFBRTtZQUNyQixLQUFLLENBQUMsS0FBSyxHQUFHLEtBQUssSUFBSSxFQUFFOztnQkFDckIsT0FBQSxNQUFNLEtBQUssQ0FBQyxPQUFPLENBQ2YsR0FBRyxRQUNILEtBQUssQ0FBQyxXQUFXLG1DQUFJLENBQUMsRUFDdEIsS0FBSyxDQUFDLFFBQVEsQ0FDakIsQ0FBQTthQUFBLENBQUE7WUFDTCxPQUFPLENBQUMsR0FBRyxFQUFFLEtBQUssQ0FBVSxDQUFBO1NBQy9CO2FBQU0sSUFBSSxXQUFXLElBQUksS0FBSyxFQUFFO1lBQzdCLEtBQUssQ0FBQyxLQUFLLEdBQUcsS0FBSyxJQUFJLEVBQUU7O2dCQUNyQixPQUFBLE1BQU0sS0FBSyxDQUFDLE9BQU8sQ0FDZixHQUFHLFFBQ0gsS0FBSyxDQUFDLFdBQVcsbUNBQUksQ0FBQyxFQUN0QixLQUFLLENBQUMsU0FBUyxDQUNsQixDQUFBO2FBQUEsQ0FBQTtZQUNMLE9BQU8sQ0FBQyxHQUFHLEVBQUUsS0FBSyxDQUFVLENBQUE7U0FDL0I7YUFBTTtZQUNILE1BQU0sSUFBSSxLQUFLLENBQ1gsNkRBQTZELENBQ2hFLENBQUE7U0FDSjtJQUNMLENBQUMsQ0FBQyxDQUdMLENBQUE7SUFFRCxNQUFNLEtBQUssR0FBRyxLQUFLLEVBQUUsR0FBRyxNQUFtQixFQUFFLEVBQUU7UUFDM0MsTUFBTSxPQUFPLENBQUMsR0FBRyxDQUNiLE1BQU0sQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDO2FBQ2YsTUFBTSxDQUNILENBQUMsQ0FBQyxJQUFJLENBQUMsRUFBRSxFQUFFLENBQUMsTUFBTSxDQUFDLE1BQU0sS0FBSyxDQUFDLElBQUksTUFBTSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsQ0FDM0Q7YUFDQSxHQUFHLENBQUMsS0FBSyxFQUFFLENBQUMsRUFBRSxLQUFLLENBQUMsRUFBRSxFQUFFO1lBQ3JCLE1BQU0sS0FBSyxDQUFDLEtBQUssRUFBRSxDQUFBO1FBQ3ZCLENBQUMsQ0FBQyxDQUNULENBQUE7SUFDTCxDQUFDLENBQUE7SUFFRCxPQUFPLENBQUMsSUFBSSxFQUFFLEtBQUssQ0FBVSxDQUFBO0FBQ2pDLENBQUMsQ0FBQSJ9