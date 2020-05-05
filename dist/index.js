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
    const info = Object.fromEntries(Object.entries(queues).map(([key, value_]) => {
        const queue = (redisUrl === undefined
            ? new bull_1.default(key)
            : new bull_1.default(key, redisUrl));
        const value = value_;
        queue.save = async (id, ...data) => {
            if (!value.storageKey) {
                throw new Error(`No storage key provided! Can't save.`);
            }
            if (!client) {
                client = await mongodb_1.MongoClient.connect("", mongo);
            }
            const collection = client
                .db(dbName)
                .collection(value.storageKey);
            await collection.bulkWrite(data.map(($set) => ({
                updateOne: {
                    filter: { _id: id },
                    update: { $set },
                    upsert: true,
                },
            })));
        };
        if ("callback" in value) {
            queue.start = async () => await queue.process(key, value.concurrency, value.callback);
            return [key, queue];
        }
        else if ("processor" in value) {
            queue.start = async () => await queue.process(key, value.concurrency, value.processor);
            return [key, queue];
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
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvaW5kZXgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSx1REFBdUQ7QUFDdkQsZ0RBQW9FO0FBQ3BFLHFDQUF1RDtBQTZCMUMsUUFBQSxZQUFZLEdBQUcsQ0FDeEIsTUFBK0MsRUFDL0MsRUFBQyxNQUFNLEdBQUcsU0FBUyxFQUFFLEtBQUssRUFBRSxRQUFRLEtBQXdCLEVBQUUsRUFDM0MsRUFBRTtJQUNyQixJQUFJLE1BQStCLENBQUE7SUFFbkMsTUFBTSxJQUFJLEdBQUksTUFBTSxDQUFDLFdBQVcsQ0FDNUIsTUFBTSxDQUFDLE9BQU8sQ0FBQyxNQUFNLENBQUMsQ0FBQyxHQUFHLENBQUMsQ0FBQyxDQUFDLEdBQUcsRUFBRSxNQUFNLENBQUMsRUFBRSxFQUFFO1FBQ3pDLE1BQU0sS0FBSyxHQUFJLENBQUMsUUFBUSxLQUFLLFNBQVM7WUFDbEMsQ0FBQyxDQUFDLElBQUksY0FBZ0IsQ0FBQyxHQUFHLENBQUM7WUFDM0IsQ0FBQyxDQUFDLElBQUksY0FBZ0IsQ0FDaEIsR0FBRyxFQUNILFFBQVEsQ0FDWCxDQUFrQyxDQUFBO1FBRXpDLE1BQU0sS0FBSyxHQUFHLE1BQThCLENBQUE7UUFFNUMsS0FBSyxDQUFDLElBQUksR0FBRyxLQUFLLEVBQUUsRUFBRSxFQUFFLEdBQUcsSUFBSSxFQUFFLEVBQUU7WUFDL0IsSUFBSSxDQUFDLEtBQUssQ0FBQyxVQUFVLEVBQUU7Z0JBQ25CLE1BQU0sSUFBSSxLQUFLLENBQUMsc0NBQXNDLENBQUMsQ0FBQTthQUMxRDtZQUVELElBQUksQ0FBQyxNQUFNLEVBQUU7Z0JBQ1QsTUFBTSxHQUFHLE1BQU0scUJBQVcsQ0FBQyxPQUFPLENBQUMsRUFBRSxFQUFFLEtBQUssQ0FBQyxDQUFBO2FBQ2hEO1lBQ0QsTUFBTSxVQUFVLEdBQUcsTUFBTTtpQkFDcEIsRUFBRSxDQUFDLE1BQU0sQ0FBQztpQkFDVixVQUFVLENBQUMsS0FBSyxDQUFDLFVBQVUsQ0FBQyxDQUFBO1lBQ2pDLE1BQU0sVUFBVSxDQUFDLFNBQVMsQ0FDdEIsSUFBSSxDQUFDLEdBQUcsQ0FBQyxDQUFDLElBQUksRUFBRSxFQUFFLENBQUMsQ0FBQztnQkFDaEIsU0FBUyxFQUFFO29CQUNQLE1BQU0sRUFBRSxFQUFDLEdBQUcsRUFBRSxFQUFFLEVBQUM7b0JBQ2pCLE1BQU0sRUFBRSxFQUFDLElBQUksRUFBQztvQkFDZCxNQUFNLEVBQUUsSUFBSTtpQkFDZjthQUNKLENBQUMsQ0FBQyxDQUNOLENBQUE7UUFDTCxDQUFDLENBQUE7UUFFRCxJQUFJLFVBQVUsSUFBSSxLQUFLLEVBQUU7WUFDckIsS0FBSyxDQUFDLEtBQUssR0FBRyxLQUFLLElBQUksRUFBRSxDQUNyQixNQUFNLEtBQUssQ0FBQyxPQUFPLENBQUMsR0FBRyxFQUFFLEtBQUssQ0FBQyxXQUFXLEVBQUUsS0FBSyxDQUFDLFFBQVEsQ0FBQyxDQUFBO1lBQy9ELE9BQU8sQ0FBQyxHQUFHLEVBQUUsS0FBSyxDQUFVLENBQUE7U0FDL0I7YUFBTSxJQUFJLFdBQVcsSUFBSSxLQUFLLEVBQUU7WUFDN0IsS0FBSyxDQUFDLEtBQUssR0FBRyxLQUFLLElBQUksRUFBRSxDQUNyQixNQUFNLEtBQUssQ0FBQyxPQUFPLENBQUMsR0FBRyxFQUFFLEtBQUssQ0FBQyxXQUFXLEVBQUUsS0FBSyxDQUFDLFNBQVMsQ0FBQyxDQUFBO1lBQ2hFLE9BQU8sQ0FBQyxHQUFHLEVBQUUsS0FBSyxDQUFVLENBQUE7U0FDL0I7YUFBTTtZQUNILE1BQU0sSUFBSSxLQUFLLENBQ1gsNkRBQTZELENBQ2hFLENBQUE7U0FDSjtJQUNMLENBQUMsQ0FBQyxDQUdMLENBQUE7SUFFRCxNQUFNLEtBQUssR0FBRyxLQUFLLEVBQUUsR0FBRyxNQUFtQixFQUFFLEVBQUU7UUFDM0MsTUFBTSxPQUFPLENBQUMsR0FBRyxDQUNiLE1BQU0sQ0FBQyxPQUFPLENBQUMsSUFBSSxDQUFDO2FBQ2YsTUFBTSxDQUNILENBQUMsQ0FBQyxJQUFJLENBQUMsRUFBRSxFQUFFLENBQUMsTUFBTSxDQUFDLE1BQU0sS0FBSyxDQUFDLElBQUksTUFBTSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsQ0FDM0Q7YUFDQSxHQUFHLENBQUMsS0FBSyxFQUFFLENBQUMsRUFBRSxLQUFLLENBQUMsRUFBRSxFQUFFO1lBQ3JCLE1BQU0sS0FBSyxDQUFDLEtBQUssRUFBRSxDQUFBO1FBQ3ZCLENBQUMsQ0FBQyxDQUNULENBQUE7SUFDTCxDQUFDLENBQUE7SUFFRCxPQUFPLENBQUMsSUFBSSxFQUFFLEtBQUssQ0FBVSxDQUFBO0FBQ2pDLENBQUMsQ0FBQSJ9