/* eslint-disable @typescript-eslint/no-explicit-any */
import QueueConstructor, {
    Job,
    JobOptions,
    ProcessPromiseFunction,
    Queue,
} from "bull"
import {MongoClient, MongoClientOptions} from "mongodb"

type QueuesHelper<
    TTag extends string | number | symbol,
    TData extends {[K in TTag]: any}
> = {[K in TTag]: TData[K] extends {tag: K} ? TData[K] : TData[K] & {tag: K}}
export type QueueDataTypes<T> = QueuesHelper<keyof T, T>

export type QueueDefinition<TData> = {
    concurrency?: number
    storageKey?: string
} & ({callback: ProcessPromiseFunction<TData>} | {processor: string})

export interface UpdatedQueue<T> extends Queue<T> {
    push: (data: T, options?: JobOptions) => Promise<Job<T>>
    save: (...data: {id: string}[]) => Promise<void>
    start: () => Promise<void>
}

export type QueuesReturnType<T extends QueueDataTypes<any>> = readonly [
    {[K in keyof T]: UpdatedQueue<T[K]>},
    (...queues: (keyof T)[]) => Promise<void>,
]

export interface CreateQueueOptions {
    dbName?: string
    mongo?: MongoClientOptions | undefined
    redisUrl?: string
}

export const createQueues = <T extends QueueDataTypes<any>>(
    queues: {[K in keyof T]: QueueDefinition<T[K]>},
    {dbName = "scraper", mongo, redisUrl}: CreateQueueOptions = {},
): QueuesReturnType<T> => {
    let client: MongoClient | undefined

    const info = (Object.fromEntries(
        Object.entries(queues).map(([key, value_]) => {
            const queue = ((redisUrl === undefined
                ? new QueueConstructor(key)
                : new QueueConstructor(
                      key,
                      redisUrl,
                  )) as unknown) as UpdatedQueue<any>

            const value = value_ as QueueDefinition<any>

            queue.save = async (...data) => {
                if (!client) {
                    client = await MongoClient.connect("", mongo)
                }
                const collection = client
                    .db(dbName)
                    .collection(value.storageKey ?? key)
                await collection.bulkWrite(
                    data.map(($set) => ({
                        updateOne: {
                            filter: {_id: $set.id},
                            update: {$set},
                            upsert: true,
                        },
                    })),
                )
            }
            queue.push = async (data, options) =>
                await queue.add(key, data, options)

            if ("callback" in value) {
                queue.start = async () =>
                    await queue.process(
                        key,
                        value.concurrency ?? 1,
                        value.callback,
                    )
                return [key, queue] as const
            } else if ("processor" in value) {
                queue.start = async () =>
                    await queue.process(
                        key,
                        value.concurrency ?? 1,
                        value.processor,
                    )
                return [key, queue] as const
            } else {
                throw new Error(
                    `Queue definition must either have a callback or a processor`,
                )
            }
        }),
    ) as unknown) as {
        [K in keyof T]: UpdatedQueue<T[K]>
    }

    const start = async (...queues: (keyof T)[]) => {
        await Promise.all(
            Object.entries(info)
                .filter(
                    ([name]) => queues.length === 0 || queues.includes(name),
                )
                .map(async ([, queue]) => {
                    await queue.start()
                }),
        )
    }

    return [info, start] as const
}
