import dotenv from "dotenv";
dotenv.config();

export default {
    TOPIC_NAME: process.env.TOPIC_NAME || 'task-events'
};