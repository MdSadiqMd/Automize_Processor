import { PrismaClient } from "@prisma/client";
import { Kafka } from "kafkajs";

import { logger, serverConfig } from "./config";

const client = new PrismaClient();

const kafka = new Kafka({
    clientId: 'outbox-processor',
    brokers: ['localhost:9092']
});

async function main() {
    const producer = kafka.producer();
    await producer.connect();

    while (1) {
        const pendingRows = await client.taskRunOutbox.findMany({
            where: {},
            take: 10
        });
        logger.info(`Pending Rows: ${pendingRows}`);

        producer.send({
            topic: serverConfig.TOPIC_NAME,
            messages: pendingRows.map(r => {
                return {
                    value: JSON.stringify({
                        zapRunId: r.taskRunId,
                        stage: 0
                    })
                };
            })
        });

        await client.taskRunOutbox.deleteMany({
            where: {
                id: {
                    in: pendingRows.map(x => x.id)
                }
            }
        });
        await new Promise(r => setTimeout(r, 3000));
    }
}

main();

// $KAFKA_HOME/bin/kafka-console-producer.sh --topic task-events --bootstrap-server localhost:9092
// $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
// $KAFKA_HOME/bin/kafka-topics.sh --create --topic task-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
// npm start