const express = require('express');
const amqp = require('amqplib');
const winston = require('winston');

const app = express();

// Winston конфигурация
const logger = winston.createLogger({
  level: 'info', // Установка уровеня журнала на 'info'
  format: winston.format.json(), // Использования формата JSON для ведения журнала
  transports: [
    new winston.transports.Console(), // Log to the console
    new winston.transports.File({ filename: 'app.log' }), // Log to a file
  ],
});

const QUEUE_NAME = 'task_queue';
const RABBITMQ_HOST = 'amqp://localhost';

let channel;
let connection;

async function sendToQueue(message) {
  try {
    await channel.assertQueue(QUEUE_NAME, { durable: true });
    channel.sendToQueue(QUEUE_NAME, Buffer.from(message), { persistent: true });
    logger.info(`Отправлено в очередь: ${message}`);
  } catch (error) {
    logger.error(error);
  }
}

async function consumeFromQueue() {
  try {
    await channel.assertQueue(QUEUE_NAME, { durable: true });
    channel.prefetch(1);
    logger.info('Ожидание сообщений в очереди...');
    channel.consume(
      QUEUE_NAME,
      async (message) => {
        if (message === null) {
          logger.info('Сообщений в очереди больше нет');
          return; // Сообщений в очереди больше нет
        }

        const content = message.content.toString();
        logger.info(`Получено из очереди: ${content}`);
        // Обработка задания микросервисом М2
        const result = await processTask(content);
        // Отправка результата в очередь
        channel.sendToQueue(
          message.properties.replyTo,
          Buffer.from(result),
          {
            correlationId: message.properties.correlationId,
          }
        );
        channel.ack(message);
        logger.info(`Отправлено в очередь: ${result}`);
      },
      { noAck: false }
    );
  } catch (error) {
    logger.error(error);
  }
}

async function processTask(task) {
  // Обработка задания
  return 'Результат обработки задачи';
}

async function startServer() {
  try {
    connection = await amqp.connect(RABBITMQ_HOST);
    channel = await connection.createChannel();
  } catch (error) {
    logger.error(error);
  }

  app.get('/', async (req, res) => {
    try {
      const task = 'Задача для обработки';
      const correlationId = Math.random().toString();
      const result = await new Promise((resolve) => {
        // Настройка для прослушивания ответа
        channel.consume(
          QUEUE_NAME,
          (message) => {
            if (message.properties.correlationId === correlationId) {
              resolve(message.content.toString());
            }
          },
          { noAck: true }
        );

        // Задача в очередь со свойствами replyTo и корреляции
        channel.sendToQueue(QUEUE_NAME, Buffer.from(task), {
          correlationId: correlationId,
          replyTo: QUEUE_NAME, // Использование той же очереди для получения ответа
        });
      });
      res.send(result);
    } catch (error) {
      logger.error(error);
      res.status(500).send('Internal Server Error');
    }
  });

  // Потребление из очереди после того, как сервер будет готов к обработке запросов
  consumeFromQueue();

  // Слушатель событий для закрытия соединения и канала RabbitMQ при завершении работы сервера
  process.on('SIGINT', async () => {
    logger.info('\nЗакрытие сервера...');
    await channel.close();
    await connection.close();
    logger.info('Соединение и канал RabbitMQ закрыты.');
    process.exit();
  });

  app.listen(3000, () => {
    logger.info('Server started on port 3000');
  });
}

startServer();