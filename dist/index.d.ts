import Bull, { JobOptions, ProcessPromiseFunction, Queue } from "bull";
import { MongoClientOptions } from "mongodb";
declare module "bull" {
    interface Queue<T> {
        init(): Promise<void>;
        push(data: Omit<T, "tag">, options?: Bull.JobOptions): Promise<Bull.Job<T>>;
        save(...data: {
            id: string;
        }[]): Promise<void>;
    }
}
declare type QueuesHelper<TTag extends string | number | symbol, TData extends {
    [K in TTag]: any;
}> = {
    [K in TTag]: TData[K] extends {
        tag: K;
    } ? TData[K] : TData[K] & {
        tag: K;
    };
};
export declare type QueueDataTypes<T> = QueuesHelper<keyof T, T>;
export declare type QueueDefinition<TData> = {
    concurrency?: number;
    options?: JobOptions;
    storageKey?: string;
} & ({
    callback: ProcessPromiseFunction<TData>;
} | {
    processor: string;
});
export declare type QueuesReturnType<T extends QueueDataTypes<any>> = readonly [{
    [K in keyof T]: Queue<T[K]>;
}, (...queues: (keyof T)[]) => Promise<void>];
export interface CreateQueueOptions {
    dbName?: string;
    defaultOptions?: JobOptions;
    mongo: MongoClientOptions & {
        url: string;
    };
    redis: {
        url: string;
    };
}
export declare const createQueues: <T extends QueuesHelper<string | number | symbol, any>>(queues: { [K in keyof T]: QueueDefinition<T[K]>; }, { dbName, defaultOptions, mongo, redis }: CreateQueueOptions) => QueuesReturnType<T>;
export {};
//# sourceMappingURL=index.d.ts.map