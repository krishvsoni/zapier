import { Kafka } from "kafkajs";
import { PrismaClient } from "@prisma/client";
const TOPIC_NAME = "zap-events"

const prisma = new PrismaClient(); 

const kafka = new Kafka({
    clientId: 'outbox-processor-2',
    brokers: ['localhost:9092']
})

async function main() {
    const consumer = kafka.consumer({ groupId: 'main-worker' });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });
    await consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                partition,
                offset: message.offset,
                value: message.value?.toString(),
            })
            await new Promise(r => setTimeout(r, 500));
            const zapRunId = message.value?.toString();
            console.log('Processing zap', zapRunId);
            const nextAction = await prisma.actions.get({

            })

            // if(nextAction.type === 'email') {
            //     await sendEmail()
            // }
            console.log('Commiting offset', message.offset);
            await consumer.commitOffsets([
                {
                    topic: TOPIC_NAME,
                    partition: partition,
                    offset: (parseInt(message.offset) + 1).toString()
                }
            ]);
        }
    })    

}

main();