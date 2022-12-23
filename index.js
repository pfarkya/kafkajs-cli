const yargs = require('yargs');
let command
const argv = yargs
  .demandCommand(1, 1, 'You need at least one command before moving on', 'You need at most one command before moving on')
  .command({
    command: 'createTopic',
    desc: 'create topic',
    builder: {
      topicName: {
        alias: 't',
        type: 'string',
        required: true
      },
      partitions: {
        alias: 'p',
        type: 'number'
      },
      replications: {
        alias: 'r',
        type: 'number'
      }
    },
    handler: (argv) => {
      console.log('RUNNING COMMAND HANDLER')
      command = 'createTopic'
    }
  })
  .command({
    command: 'subscribeTopic',
    desc: 'Set a config variable',
    builder: {
      topicName: {
        alias: 't',
        type: 'string',
        required: true
      }
    },
    handler: (argv) => {
      console.log('RUNNING COMMAND HANDLER')
      command = 'subscribeTopic'
    }
  })
  .command({
    command: 'publishTopic',
    desc: 'Set a config variable',
    builder: {
      topicName: {
        alias: 't',
        type: 'string',
        required: true
      },
      message: {
        alias: 'm',
        type: 'string',
        required: true
      }
    },
    handler: (argv) => {
      console.log('RUNNING COMMAND HANDLER')
      command = 'publishTopic'
    }
  })
  .option('credentialType', {
    alias: 'ct',
    description: 'What type of credential do you want to use',
    type: 'string'
  })
  .option('brokers', {
    alias: 'b',
    description: 'Broker string',
    type: 'string',
    required: true
  })
  .option('region', {
    alias: 'rg',
    description: 'Region for the aws cluster',
    type: 'string'
  })
  .option('ttl', {
    description: 'time to live',
    type: 'string'
  })
  .option('username', {
    alias: 'rg',
    description: 'Region for the aws cluster',
    type: 'string'
  })
  .option('password', {
    description: 'time to live',
    type: 'string'
  })
  .option('profile', {
    alias: 'pr',
    description: 'profile for aws',
    type: 'string'
  })
  .help()
  .alias('help', 'h').argv;


console.log("RUNNING COMMAND WITH FOLLOWING OPTIONS", argv)
const { Kafka } = require('kafkajs')
const {
  awsIamAuthenticator,
  Type
} = require('@jm18457/kafkajs-msk-iam-authentication-mechanism')

const { awsIamOutAuthenticator } = require('./awsOutCluster')
let credentialType = argv.credentialType
let brokers = argv.brokers
let configuration
switch(credentialType) {
  case 'aws-iam-incluster':
    configuration = {
      clientId: 'my-app',
      brokers: brokers.split(','),
      ssl: true,
      sasl: {
        mechanism: Type,
        authenticationProvider: awsIamAuthenticator(argv.region, argv.ttl)
      }
    }
    break
  case 'aws-iam-out':
    configuration = {
      clientId: 'my-app',
      brokers: brokers.split(','),
      ssl: true,
      sasl: {
        mechanism: Type,
        authenticationProvider: awsIamOutAuthenticator(argv.region, argv.ttl, argv.profile)
      }
    }
    break
  case 'scram': 
    configuration = {
      clientId: 'my-app',
      brokers: brokers.split(','),
      // authenticationTimeout: 10000,
      // reauthenticationThreshold: 10000,
      ssl: true,
      sasl: {
        mechanism: 'SCRAM-SHA-512', // scram-sha-256 or scram-sha-512
        username: argv.username,
        password: argv.password
      },
    }
    break
  case 'eventstream':
    break
  default:
    configuration = {
      clientId: 'my-app',
      brokers: brokers.split(','),
    }
}

const kafka = new Kafka(configuration)


async function runCommand(command, options) {
  try {
    switch (command) {
      case 'createTopic':
        console.log('RUNNING COMMAND CREATETOPIC')
        const admin = kafka.admin()
        await admin.connect()
        await admin.createTopics({
            topics: [{
              topic: options.topicName,
              numPartitions: options.partitions,     // default: -1 (uses broker `num.partitions` configuration)
              replicationFactor: options.replications // default: -1 (uses broker `default.replication.factor` configuration)
          }],
        })
        console.log('RUNNING COMMAND CREATETOPIC DONE')
        break
      case 'subscribeTopic':
        const consumer = kafka.consumer({ groupId: 'test-group' })
        await consumer.connect()
        await consumer.subscribe({ topic: options.topicName, fromBeginning: true })

        await consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            console.log({
              topic: topic,
              partition: partition,
              value: message.value.toString(),
            })
          },
        })
        break
      case 'publishTopic':
        const producer = kafka.producer()
        await producer.connect()
        await producer.send({
          topic: options.topicName,
          messages: [
            { value: options.message },
          ],
        })

        await producer.disconnect()
        break
    }
  } catch(e) {
    console.error("Failed to run the comand", e)
  }
}

console.log('RUNNING COMMAND')
runCommand(command, argv)