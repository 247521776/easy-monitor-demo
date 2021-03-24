require('xprofiler').start();
const xtransit = require('xtransit');

xtransit.start({
    server: `ws://127.0.0.1:9090`, // 填写前一节中部署的 xtransit-server 地址
    appId: 2, // 创建应用得到的应用 ID
    appSecret: '37c1aaa8d860702917ebd5ad95c91d24', // 创建应用得到的应用 Secret
});

const consumer = require('@optum/knack-consumer');

(async () => {
    const kafkaConsumer = await consumer.connect({
        topics: ['validator'],
        consumerConfig: {
            'socket.keepalive.enable': true,
            'enable.auto.commit': true,
            'group.id': 'validator-consumer',
            'metadata.broker.list': 'localhost:9092'
        },
        topicConfig: {
            'auto.offset.reset': 'latest'
        },
        onData(data) {
            const value = JSON.parse(data.value.toString());
            console.log('======================');
            console.log(value);
            console.log('======================');
        },
    });

    const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

    process.on('unhandledRejection', async e => {
        console.log(`process.on unhandledRejection`);
        console.error(e);
    });

    process.on('uncaughtException', async e => {
        console.log(`process.on uncaughtException`);
        console.error(e);
    });

    signalTraps.map(type => {
        process.once(type, async () => {
            try {
                await kafkaConsumer.disconnect();
            } finally {
                process.kill(process.pid, type);
            }
        });
    });
})()