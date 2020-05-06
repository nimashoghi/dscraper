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
    const info = Object.fromEntries(Object.entries(queues).map(([tag, value_]) => {
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
            queue.start = async () => value.concurrency
                ? await queue.process(tag, value.concurrency, value.callback)
                : await queue.process(tag, value.callback);
            return [tag, queue];
        }
        else if ("processor" in value) {
            queue.start = async () => value.concurrency
                ? await queue.process(tag, value.concurrency, value.processor)
                : await queue.process(tag, value.processor);
            return [tag, queue];
        }
        else {
            throw new Error(`Queue definition must either have a callback or a processor`);
        }
    }));
    const start = async (...queues) => {
        const queues_ = new Set(queues.length === 0 ? Object.keys(info) : queues);
        await Promise.all(Object.entries(info)
            .filter(([name]) => queues_.has(name))
            .map(async ([, queue]) => {
            await queue.start();
        }));
    };
    return [info, start];
};
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvaW5kZXgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSx1REFBdUQ7QUFDdkQsZ0RBQW9FO0FBQ3BFLHFDQUF1RDtBQW9DMUMsUUFBQSxZQUFZLEdBQUcsQ0FDeEIsTUFBK0MsRUFDL0MsRUFBQyxNQUFNLEdBQUcsU0FBUyxFQUFFLEtBQUssRUFBRSxLQUFLLEVBQXFCLEVBQ25DLEVBQUU7SUFDckIsSUFBSSxNQUErQixDQUFBO0lBRW5DLE1BQU0sSUFBSSxHQUFJLE1BQU0sQ0FBQyxXQUFXLENBQzVCLE1BQU0sQ0FBQyxPQUFPLENBQUMsTUFBTSxDQUFDLENBQUMsR0FBRyxDQUFDLENBQUMsQ0FBQyxHQUFHLEVBQUUsTUFBTSxDQUFDLEVBQUUsRUFBRTtRQUN6QyxNQUFNLEtBQUssR0FBRyxJQUFJLGNBQUksQ0FBQyxHQUFHLEVBQUUsS0FBSyxDQUFDLEdBQUcsQ0FBQyxDQUFBO1FBRXRDLE1BQU0sS0FBSyxHQUFHLE1BQThCLENBQUE7UUFFNUMsS0FBSyxDQUFDLElBQUksR0FBRyxLQUFLLEVBQUUsR0FBRyxJQUFJLEVBQUUsRUFBRTs7WUFDM0IsSUFBSSxDQUFDLE1BQU0sRUFBRTtnQkFDVCxNQUFNLEdBQUcsTUFBTSxxQkFBVyxDQUFDLE9BQU8sQ0FBQyxLQUFLLENBQUMsR0FBRyxFQUFFO29CQUMxQyxlQUFlLEVBQUUsSUFBSTtvQkFDckIsR0FBRyxLQUFLO2lCQUNYLENBQUMsQ0FBQTthQUNMO1lBQ0QsTUFBTSxVQUFVLEdBQUcsTUFBTTtpQkFDcEIsRUFBRSxDQUFDLE1BQU0sQ0FBQztpQkFDVixVQUFVLE9BQUMsS0FBSyxDQUFDLFVBQVUsbUNBQUksR0FBRyxDQUFDLENBQUE7WUFDeEMsTUFBTSxVQUFVLENBQUMsU0FBUyxDQUN0QixJQUFJLENBQUMsR0FBRyxDQUFDLENBQUMsSUFBSSxFQUFFLEVBQUUsQ0FBQyxDQUFDO2dCQUNoQixTQUFTLEVBQUU7b0JBQ1AsTUFBTSxFQUFFLEVBQUMsR0FBRyxFQUFFLElBQUksQ0FBQyxFQUFFLEVBQUM7b0JBQ3RCLE1BQU0sRUFBRSxFQUFDLElBQUksRUFBQztvQkFDZCxNQUFNLEVBQUUsSUFBSTtpQkFDZjthQUNKLENBQUMsQ0FBQyxDQUNOLENBQUE7UUFDTCxDQUFDLENBQUE7UUFDRCxLQUFLLENBQUMsSUFBSSxHQUFHLEtBQUssRUFBRSxJQUFJLEVBQUUsT0FBTyxFQUFFLEVBQUU7O1lBQ2pDLE9BQUEsTUFBTSxLQUFLLENBQUMsR0FBRyxDQUNYLEdBQUcsRUFDSCxFQUFDLEdBQUcsSUFBSSxFQUFFLEdBQUcsRUFBQyxFQUNkLEVBQUMsR0FBRyxPQUFDLEtBQUssQ0FBQyxPQUFPLG1DQUFJLEVBQUUsQ0FBQyxFQUFFLEdBQUcsQ0FBQyxPQUFPLGFBQVAsT0FBTyxjQUFQLE9BQU8sR0FBSSxFQUFFLENBQUMsRUFBQyxDQUNqRCxDQUFBO1NBQUEsQ0FBQTtRQUVMLElBQUksVUFBVSxJQUFJLEtBQUssRUFBRTtZQUNyQixLQUFLLENBQUMsS0FBSyxHQUFHLEtBQUssSUFBSSxFQUFFLENBQ3JCLEtBQUssQ0FBQyxXQUFXO2dCQUNiLENBQUMsQ0FBQyxNQUFNLEtBQUssQ0FBQyxPQUFPLENBQ2YsR0FBRyxFQUNILEtBQUssQ0FBQyxXQUFXLEVBQ2pCLEtBQUssQ0FBQyxRQUFRLENBQ2pCO2dCQUNILENBQUMsQ0FBQyxNQUFNLEtBQUssQ0FBQyxPQUFPLENBQUMsR0FBRyxFQUFFLEtBQUssQ0FBQyxRQUFRLENBQUMsQ0FBQTtZQUNsRCxPQUFPLENBQUMsR0FBRyxFQUFFLEtBQUssQ0FBVSxDQUFBO1NBQy9CO2FBQU0sSUFBSSxXQUFXLElBQUksS0FBSyxFQUFFO1lBQzdCLEtBQUssQ0FBQyxLQUFLLEdBQUcsS0FBSyxJQUFJLEVBQUUsQ0FDckIsS0FBSyxDQUFDLFdBQVc7Z0JBQ2IsQ0FBQyxDQUFDLE1BQU0sS0FBSyxDQUFDLE9BQU8sQ0FDZixHQUFHLEVBQ0gsS0FBSyxDQUFDLFdBQVcsRUFDakIsS0FBSyxDQUFDLFNBQVMsQ0FDbEI7Z0JBQ0gsQ0FBQyxDQUFDLE1BQU0sS0FBSyxDQUFDLE9BQU8sQ0FBQyxHQUFHLEVBQUUsS0FBSyxDQUFDLFNBQVMsQ0FBQyxDQUFBO1lBQ25ELE9BQU8sQ0FBQyxHQUFHLEVBQUUsS0FBSyxDQUFVLENBQUE7U0FDL0I7YUFBTTtZQUNILE1BQU0sSUFBSSxLQUFLLENBQ1gsNkRBQTZELENBQ2hFLENBQUE7U0FDSjtJQUNMLENBQUMsQ0FBQyxDQUdMLENBQUE7SUFFRCxNQUFNLEtBQUssR0FBRyxLQUFLLEVBQUUsR0FBRyxNQUFtQixFQUFFLEVBQUU7UUFDM0MsTUFBTSxPQUFPLEdBQUcsSUFBSSxHQUFHLENBQ25CLE1BQU0sQ0FBQyxNQUFNLEtBQUssQ0FBQyxDQUFDLENBQUMsQ0FBRSxNQUFNLENBQUMsSUFBSSxDQUFDLElBQUksQ0FBaUIsQ0FBQyxDQUFDLENBQUMsTUFBTSxDQUNwRSxDQUFBO1FBQ0QsTUFBTSxPQUFPLENBQUMsR0FBRyxDQUNiLE1BQU0sQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDO2FBQ2YsTUFBTSxDQUFDLENBQUMsQ0FBQyxJQUFJLENBQUMsRUFBRSxFQUFFLENBQUMsT0FBTyxDQUFDLEdBQUcsQ0FBQyxJQUFJLENBQUMsQ0FBQzthQUNyQyxHQUFHLENBQUMsS0FBSyxFQUFFLENBQUMsRUFBRSxLQUFLLENBQUMsRUFBRSxFQUFFO1lBQ3JCLE1BQU0sS0FBSyxDQUFDLEtBQUssRUFBRSxDQUFBO1FBQ3ZCLENBQUMsQ0FBQyxDQUNULENBQUE7SUFDTCxDQUFDLENBQUE7SUFFRCxPQUFPLENBQUMsSUFBSSxFQUFFLEtBQUssQ0FBVSxDQUFBO0FBQ2pDLENBQUMsQ0FBQSJ9