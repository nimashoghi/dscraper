/* eslint-disable @typescript-eslint/no-explicit-any */
import QueueConstructor, {ProcessPromiseFunction, Queue} from "bull"
import {MongoClient, MongoClientOptions} from "mongodb"

type QueuesHelper<
    TTag extends string | number | symbol,
    TData extends {[K in TTag]: any}
> = {[K in TTag]: TData[K] extends {tag: K} ? TData[K] : TData[K] & {tag: K}}
export type Queues<T> = QueuesHelper<keyof T, T>

export type QueueDefinition<TData> = {
    concurrency: number
    storageKey?: string
} & ({callback: ProcessPromiseFunction<TData>} | {processor: string})

export interface UpdatedQueue<T> extends Queue<T> {
    save: (id: string, ...data: any[]) => Promise<void>
    start: () => Promise<void>
}

export type QueuesReturnType<T extends Queues<any>> = readonly [
    {[K in keyof T]: UpdatedQueue<T[K]>},
    (...queues: (keyof T)[]) => Promise<void>,
]

export interface CreateQueueOptions {
    dbName?: string
    mongo?: MongoClientOptions | undefined
    redisUrl?: string
}

export const createQueues = <T extends Queues<any>>(
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

            queue.save = async (id, ...data) => {
                if (!value.storageKey) {
                    throw new Error(`No storage key provided! Can't save.`)
                }

                if (!client) {
                    client = await MongoClient.connect("", mongo)
                }
                const collection = client
                    .db(dbName)
                    .collection(value.storageKey)
                await collection.bulkWrite(
                    data.map(($set) => ({
                        updateOne: {
                            filter: {_id: id},
                            update: {$set},
                            upsert: true,
                        },
                    })),
                )
            }

            if ("callback" in value) {
                queue.start = async () =>
                    await queue.process(key, value.concurrency, value.callback)
                return [key, queue] as const
            } else if ("processor" in value) {
                queue.start = async () =>
                    await queue.process(key, value.concurrency, value.processor)
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
