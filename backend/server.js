const express = require('express');
const redis = require('redis');
const cors = require('cors');

const app = express();
app.use(cors());

const client = redis.createClient({ url: 'redis://localhost:6379' });
client.connect();

app.get('/', (req, res) => {
  res.send('Taxi Backend API is running.');
});

app.get('/api/taxi/:id', async (req, res) => {
  const taxiId = req.params.id;
  try {
    const speed = await client.get(`taxi:${taxiId}:average_speed`);
    res.json({ taxiId, speed });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.listen(3001, () => {
  console.log('Backend running on http://localhost:3001');
});
