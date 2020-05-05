import { Job, JobOptions, ProcessPromiseFunction, Queue } from "bull";
import { MongoClientOptions } from "mongodb";
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
    storageKey?: string;
} & ({
    callback: ProcessPromiseFunction<TData>;
} | {
    processor: string;
});
export interface UpdatedQueue<T> extends Queue<T> {
    push: (data: Omit<T, "tag">, options?: JobOptions) => Promise<Job<T>>;
    save: (...data: {
        id: string;
    }[]) => Promise<void>;
    start: () => Promise<void>;
}
export declare type QueuesReturnType<T extends QueueDataTypes<any>> = readonly [{
    [K in keyof T]: UpdatedQueue<T[K]>;
}, (...queues: (keyof T)[]) => Promise<void>];
export interface CreateQueueOptions {
    dbName?: string;
    mongo?: MongoClientOptions | undefined;
    redisUrl?: string;
}
export declare const createQueues: <T extends QueuesHelper<string | number | symbol, any>>(queues: { [K in keyof T]: QueueDefinition<T[K]>; }, { dbName, mongo, redisUrl }?: CreateQueueOptions) => QueuesReturnType<T>;
export {};
//# sourceMappingURL=index.d.ts.map