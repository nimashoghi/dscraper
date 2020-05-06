/* eslint-disable @typescript-eslint/no-explicit-any */
import Bull, {JobOptions, ProcessPromiseFunction, Queue} from "bull"
import {MongoClient, MongoClientOptions} from "mongodb"

declare module "bull" {
    export interface Queue<T> {
        push(
            data: Omit<T, "tag">,
            options?: Bull.JobOptions,
        ): Promise<Bull.Job<T>>
        save(...data: {id: string}[]): Promise<void>
        start(): Promise<void>
    }
}

type QueuesHelper<
    TTag extends string | number | symbol,
    TData extends {[K in TTag]: any}
> = {[K in TTag]: TData[K] extends {tag: K} ? TData[K] : TData[K] & {tag: K}}
export type QueueDataTypes<T> = QueuesHelper<keyof T, T>

export type QueueDefinition<TData> = {
    concurrency?: number
    options?: JobOptions
    storageKey?: string
} & ({callback: ProcessPromiseFunction<TData>} | {processor: string})

export type QueuesReturnType<T extends QueueDataTypes<any>> = readonly [
    {[K in keyof T]: Queue<T[K]>},
    (...queues: (keyof T)[]) => Promise<void>,
]

export interface CreateQueueOptions {
    dbName?: string
    mongo: MongoClientOptions & {url: string}
    redis: {url: string}
}

export const createQueues = <T extends QueueDataTypes<any>>(
    queues: {[K in keyof T]: QueueDefinition<T[K]>},
    {dbName = "scraper", mongo, redis}: CreateQueueOptions,
): QueuesReturnType<T> => {
    let client: MongoClient | undefined

    const info = (Object.fromEntries(
        Object.entries(queues).map(([tag, value_]) => {
            const queue = new Bull(tag, redis.url)

            const value = value_ as QueueDefinition<any>

            queue.save = async (...data) => {
                if (!client) {
                    client = await MongoClient.connect(mongo.url, {
                        useNewUrlParser: true,
                        ...mongo,
                    })
                }
                const collection = client
                    .db(dbName)
                    .collection(value.storageKey ?? tag)
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
                await queue.add(
                    tag,
                    {...data, tag},
                    {...(value.options ?? {}), ...(options ?? {})},
                )

            if ("callback" in value) {
                queue.start = async () =>
                    value.concurrency
                        ? await queue.process(
                              tag,
                              value.concurrency,
                              value.callback,
                          )
                        : await queue.process(tag, value.callback)
                return [tag, queue] as const
            } else if ("processor" in value) {
                queue.start = async () =>
                    value.concurrency
                        ? await queue.process(
                              tag,
                              value.concurrency,
                              value.processor,
                          )
                        : await queue.process(tag, value.processor)
                return [tag, queue] as const
            } else {
                throw new Error(
                    `Queue definition must either have a callback or a processor`,
                )
            }
        }),
    ) as unknown) as {
        [K in keyof T]: Queue<T[K]>
    }

    const start = async (...queues: (keyof T)[]) => {
        const queues_ = new Set<keyof T>(
            queues.length === 0 ? (Object.keys(info) as (keyof T)[]) : queues,
        )
        await Promise.all(
            Object.entries(info)
                .filter(([name]) => queues_.has(name))
                .map(async ([, queue]) => {
                    await queue.start()
                }),
        )
    }

    return [info, start] as const
}
