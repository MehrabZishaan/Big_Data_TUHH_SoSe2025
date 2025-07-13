import express from 'express';
import redis from 'redis';
import cors from 'cors';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { time } from 'console';

const app = express();
const server = createServer(app);
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// Middleware
app.use(cors({ origin: '*' }));
app.use(express.json());

const redisUrl = `redis://${process.env.REDIS_HOST || 'localhost'}:${process.env.REDIS_PORT || 6379}`;

const redisClient = await redis.createClient({ url: redisUrl }).connect();


// await redisClient.connect();

// const redisClient = await redis.createClient({
//   host: process.env.REDIS_HOST || 'localhost',
//   port: process.env.REDIS_PORT || 6379
// }).connect();

// Connect to Redis

redisClient.on('error', (err) => {
  console.error('Redis Client Error', err);

});

redisClient.on('connect', () => {
  console.log('Connected to Redis');
});



// Connect to Redis
// redisClient.connect();

// API Routes
app.get('/api/taxi-locations', async (req, res) => {
  try {
    const locations = await redisClient.hGetAll('taxi_locations');
    const formattedLocations = Object.entries(locations).map(([taxiId, locationData]) => {
      const [lat, lng, timestamp] = locationData.split(',');
      return {
        taxiId,
        latitude: parseFloat(lat),
        longitude: parseFloat(lng),
        timestamp: parseInt(timestamp)
      };
    });
    res.json(formattedLocations);
  } catch (error) {
    console.error('Error fetching taxi locations:', error);
    res.status(500).json({ error: 'Failed to fetch taxi locations' });
  }
});

app.get('/api/speeds', async (req, res) => {
  try {
    const speeds = await redisClient.hGetAll('speed');
    const formattedSpeeds = Object.entries(speeds).map(([taxiId, speed]) => ({
      taxiId,
      speed: parseFloat(speed)
    }));
    res.json(formattedSpeeds);
  } catch (error) {
    console.error('Error fetching speeds:', error);
    res.status(500).json({ error: 'Failed to fetch speeds' });
  }
});

app.get('/api/average-speeds', async (req, res) => {
  try {
    const avgSpeeds = await redisClient.hGetAll('average_speed');
    const formattedAvgSpeeds = Object.entries(avgSpeeds).map(([taxiId, avgSpeed]) => ({
      taxiId,
      averageSpeed: parseFloat(avgSpeed)
    }));
    res.json(formattedAvgSpeeds);
  } catch (error) {
    console.error('Error fetching average speeds:', error);
    res.status(500).json({ error: 'Failed to fetch average speeds' });
  }
});

app.get('/api/distances', async (req, res) => {
  try {
    const distances = await redisClient.hGetAll('taxi_distance');
    const formattedDistances = Object.entries(distances).map(([taxiId, distance]) => ({
      taxiId,
      distance: parseFloat(distance)
    }));
    res.json(formattedDistances);
  } catch (error) {
    console.error('Error fetching distances:', error);
    res.status(500).json({ error: 'Failed to fetch distances' });
  }
});

app.get('/api/alerts', async (req, res) => {
  try {
    const alerts = await redisClient.lRange('alerts', 0, -1);
    res.json(alerts.reverse()); // Most recent first
  } catch (error) {
    console.error('Error fetching alerts:', error);
    res.status(500).json({ error: 'Failed to fetch alerts' });
  }
});

app.get('/api/statistics', async (req, res) => {
  try {
    const [
      totalSpeedingTaxis,
      totalAreaViolations,
      totalDistance,
      currentlyDrivingTaxis,
      speedingIncidents,
      currentViolations
    ] = await Promise.all([
      redisClient.get('total_speeding_taxis'),
      redisClient.get('total_area_violations'),
      redisClient.get('total_distance_all_taxis'),
      redisClient.get('currently_driving_taxis'),
      redisClient.hGetAll('speeding_incidents'),
      redisClient.hGetAll('current_violations')
    ]);

    res.json({
      totalSpeedingTaxis: parseInt(totalSpeedingTaxis) || 0,
      totalAreaViolations: parseInt(totalAreaViolations) || 0,
      totalDistance: parseFloat(totalDistance) || 0,
      currentlyDrivingTaxis: parseInt(currentlyDrivingTaxis) || 0,
      speedingIncidents: Object.entries(speedingIncidents).map(([taxiId, speed]) => ({
        taxiId,
        speed
      })),
      currentViolations: Object.entries(currentViolations).map(([taxiId, timestamp]) => ({
        taxiId,
        timestamp: parseInt(timestamp)
      }))
    });
  } catch (error) {
    console.error('Error fetching statistics:', error);
    res.status(500).json({ error: 'Failed to fetch statistics' });
  }
});

// Socket.IO for real-time updates
io.on('connection', (socket) => {
  console.log('Client connected:', socket.id);
  socket.on('send_message', (data) => {
    console.log('Message received:', data);
    io.emit('receive_message', data); // Broadcast to all clients
  });

  socket.on('disconnect', () => {
    console.log('Client disconnected:', socket.id);
  });
});

// Real-time data broadcasting
const broadcastData = async () => {
  console.log('Broadcasting data to clients from redis...');
  try {
    const [locations, speeds, alerts, statistics] = await Promise.all([
      redisClient.hGetAll('taxi_locations'),
      redisClient.hGetAll('speed'),
      redisClient.lRange('alerts', 0, 100), // Last 10 alerts
      (async () => {
        const [
          totalSpeedingTaxis,
          totalAreaViolations,
          totalDistance,
          currentlyDrivingTaxis,
          speedingIncidents,
          currentViolations
        ] = await Promise.all([
          redisClient.get('total_speeding_taxis'),
          redisClient.get('total_area_violations'),
          redisClient.get('total_distance_all_taxis'),
          redisClient.get('currently_driving_taxis'),
          redisClient.hGetAll('speeding_incidents'),
          redisClient.hGetAll('current_violations')
        ]);

        return {
          totalSpeedingTaxis: parseInt(totalSpeedingTaxis) || 0,
          totalAreaViolations: parseInt(totalAreaViolations) || 0,
          totalDistance: parseFloat(totalDistance) || 0,
          currentlyDrivingTaxis: parseInt(currentlyDrivingTaxis) || 0,
          speedingIncidents: Object.entries(speedingIncidents).map(([taxiId, speed]) => ({
            taxiId,
            speed
          })),
          currentViolations: Object.entries(currentViolations).map(([taxiId, timestamp]) => ({
            taxiId,
            timestamp: parseInt(timestamp)
          }))
        };
      })()
    ]);

    const formattedLocations = Object.entries(locations).map(([taxiId, locationData]) => {
      const [lat, lng, timestamp] = locationData.split(',');
      return {
        taxiId,
        latitude: parseFloat(lat),
        longitude: parseFloat(lng),
        timestamp: parseInt(timestamp)
      };
    });

    const formattedSpeeds = Object.entries(speeds).map(([taxiId, speed]) => ({
      taxiId,
      speed: parseFloat(speed)
    }));

    io.emit('realTimeData', {
      locations: formattedLocations,
      speeds: formattedSpeeds,
      alerts: alerts.reverse(),
      statistics
    });
  } catch (error) {
    console.error('Error broadcasting data:', error);
  }
};

// Broadcast data every 5 seconds
setInterval(broadcastData, 5000);

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});