import React, { useState, useEffect } from 'react';
import { MapContainer, TileLayer, Marker, Circle, Popup } from 'react-leaflet';
import { io } from 'socket.io-client';
import L from 'leaflet';

// Fix for default markers in react-leaflet
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
});

// Custom icons for different taxi states
const normalTaxiIcon = new L.Icon({
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  shadowSize: [41, 41]
});

const speedingTaxiIcon = new L.Icon({
  iconUrl: 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjUiIGhlaWdodD0iNDEiIHZpZXdCb3g9IjAgMCAyNSA0MSIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTEyLjUgMEwyMi41IDI1SDE1VjQxTDIuNSAyNUg5VjBIMTIuNVoiIGZpbGw9IiNGRjAwMDAiLz4KPHN2Zz4K',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34]
});

const violatingTaxiIcon = new L.Icon({
  iconUrl: 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMjUiIGhlaWdodD0iNDEiIHZpZXdCb3g9IjAgMCAyNSA0MSIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPHBhdGggZD0iTTEyLjUgMEwyMi41IDI1SDE1VjQxTDIuNSAyNUg5VjBIMTIuNVoiIGZpbGw9IiNGRkE1MDAiLz4KPHN2Zz4K',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34]
});

const TaxiFleetDashboard = () => {
  const [taxiLocations, setTaxiLocations] = useState([]);
  const [speeds, setSpeeds] = useState([]);
  const [alerts, setAlerts] = useState([]);
  const [statistics, setStatistics] = useState({
    totalSpeedingTaxis: 0,
    totalAreaViolations: 0,
    totalDistance: 0,
    currentlyDrivingTaxis: 0,
    speedingIncidents: [],
    currentViolations: []
  });
  const [connected, setConnected] = useState(false);

  // Forbidden City coordinates and geofence circles
  const FORBIDDEN_CITY = { lat: 39.916, lng: 116.397 };
  const WARNING_RADIUS = 10000; // 10km in meters
  const DROP_RADIUS = 15000; // 15km in meters

  useEffect(() => {
    const BASE_URL = `${window.location.protocol}//${window.location.hostname}:5000`;
    const socket = io(BASE_URL);

    socket.on('connect', () => {
      setConnected(true);
      console.log('Connected to server');
    });

    socket.on('disconnect', () => {
      setConnected(false);
      console.log('Disconnected from server');
    });

    socket.on('receive_message', (data) => {
      console.log('Message received:', data);
    });
    socket.on('send_message', (data) => {
      console.log('Message received:', data);
    });

    socket.on('realTimeData', (data) => {
      console.log('Received real-time data:', data);
      setTaxiLocations(data.locations);
      setSpeeds(data.speeds);
      setAlerts(data.alerts);
      setStatistics(data.statistics);
    });

    // Initial data fetch
    fetchInitialData(BASE_URL);

    return () => socket.disconnect();
  }, []);

  const fetchInitialData = async (BASE_URL) => {
    try {
      const [locationsRes, speedsRes, alertsRes, statsRes] = await Promise.all([
        fetch(`${BASE_URL}/api/taxi-locations`),
        fetch(`${BASE_URL}/api/speeds`),
        fetch(`${BASE_URL}/api/alerts`),
        fetch(`${BASE_URL}/api/statistics`)
      ]);

      const locations = await locationsRes.json();
      const speeds = await speedsRes.json();
      const alerts = await alertsRes.json();
      const stats = await statsRes.json();

      setTaxiLocations(locations);
      setSpeeds(speeds);
      setAlerts(alerts);
      setStatistics(stats);
    } catch (error) {
      console.error('Error fetching initial data:', error);
    }
  };

  const getTaxiIcon = (taxiId) => {
    const isSpeeding = statistics.speedingIncidents.some(incident => incident.taxiId === taxiId);
    const isViolating = statistics.currentViolations.some(violation => violation.taxiId === taxiId);

    if (isSpeeding) return speedingTaxiIcon;
    if (isViolating) return violatingTaxiIcon;
    return normalTaxiIcon;
  };

  const getTaxiSpeed = (taxiId) => {
    const speedData = speeds.find(s => s.taxiId === taxiId);
    return speedData ? speedData.speed.toFixed(1) : 'N/A';
  };

  const formatAlert = (alert) => {
    if (alert.includes('SPEEDING')) {
      return { type: 'speeding', message: alert, color: '#ff4444' };
    } else if (alert.includes('GEOFENCE')) {
      return { type: 'geofence', message: alert, color: '#ff8800' };
    }
    return { type: 'other', message: alert, color: '#666' };
  };

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="px-6 py-4">
          <div className="flex items-center justify-between">
            <h1 className="text-2xl font-bold text-gray-900">Taxi Fleet Monitoring</h1>
            <div className="flex items-center space-x-2">
              <div className={`w-3 h-3 rounded-full ${connected ? 'bg-green-500' : 'bg-red-500'}`}></div>
              <span className={`text-sm ${connected ? 'text-green-600' : 'text-red-600'}`}>
                {connected ? 'Connected' : 'Disconnected'}
              </span>
            </div>
          </div>
        </div>
      </div>

      <div className="flex h-full">
        {/* Main Map Area */}
        <div className="flex-1 p-4">
          <div className="bg-white rounded-lg shadow h-full">
            <div className="p-4 border-b">
              <h2 className="text-lg font-semibold">Live Taxi Locations</h2>
            </div>
            <div className="h-96" >
              {(true && <MapContainer
                center={[FORBIDDEN_CITY.lat, FORBIDDEN_CITY.lng]}
                zoom={11}
              // style={{ height: '100%', width: '100%' }}
              >
                <TileLayer
                  url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
                  attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                />

                {/* Geofence circles */}
                <Circle
                  center={[FORBIDDEN_CITY.lat, FORBIDDEN_CITY.lng]}
                  radius={WARNING_RADIUS}
                  color="#ff8800"
                  fillColor="#ff8800"
                  fillOpacity={0.1}
                  weight={2}
                />
                <Circle
                  center={[FORBIDDEN_CITY.lat, FORBIDDEN_CITY.lng]}
                  radius={DROP_RADIUS}
                  color="#ff4444"
                  fillColor="#ff4444"
                  fillOpacity={0.05}
                  weight={2}
                />

                {/* Taxi markers */}
                {taxiLocations.map((taxi) => (
                  <Marker
                    key={taxi.taxiId}
                    position={[taxi.latitude, taxi.longitude]}
                    icon={getTaxiIcon(taxi.taxiId)}
                  >
                    <Popup>
                      <div className="text-sm">
                        <div className="font-semibold">Taxi {taxi.taxiId}</div>
                        <div>Speed: {getTaxiSpeed(taxi.taxiId)} km/h</div>
                        <div>Location: {taxi.latitude.toFixed(6)}, {taxi.longitude.toFixed(6)}</div>
                        <div>Last Update: {new Date(taxi.timestamp).toLocaleTimeString()}</div>
                      </div>
                    </Popup>
                  </Marker>
                ))}
              </MapContainer>
              )}
            </div>
          </div>
        </div>

        {/* Sidebar */}
        <div className="w-80 p-4 space-y-4">
          {/* Statistics Cards */}
          <div className="bg-white rounded-lg shadow p-4">
            <h3 className="text-lg font-semibold mb-4">Fleet Statistics</h3>
            <div className="space-y-3">
              <div className="flex justify-between">
                <span className="text-gray-600">Currently Driving</span>
                <span className="font-semibold text-blue-600">{statistics.currentlyDrivingTaxis}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-gray-600">Total Distance</span>
                <span className="font-semibold">{statistics.totalDistance.toFixed(2)} km</span>
              </div>
              <div className="flex justify-between">
                <span className="text-red-600">Speeding Taxis</span>
                <span className="font-semibold text-red-600">{statistics.totalSpeedingTaxis}</span>
              </div>
              <div className="flex justify-between">
                <span className="text-orange-600">Area Violations</span>
                <span className="font-semibold text-orange-600">{statistics.totalAreaViolations}</span>
              </div>
            </div>
          </div>

          {/* Current Incidents */}
          <div className="bg-white rounded-lg shadow p-4">
            <h3 className="text-lg font-semibold mb-4">Current Incidents</h3>
            <div className="space-y-2 max-h-32 overflow-y-auto">
              {statistics.speedingIncidents.map((incident, index) => (
                <div key={index} className="text-sm p-2 bg-red-50 rounded border-l-2 border-red-400">
                  <div className="font-medium text-red-700">Taxi {incident.taxiId}</div>
                  <div className="text-red-600">Speeding: {incident.speed}</div>
                </div>
              ))}
              {statistics.currentViolations.map((violation, index) => (
                <div key={index} className="text-sm p-2 bg-orange-50 rounded border-l-2 border-orange-400">
                  <div className="font-medium text-orange-700">Taxi {violation.taxiId}</div>
                  <div className="text-orange-600">Area Violation</div>
                </div>
              ))}
            </div>
          </div>

          {/* Recent Alerts */}
          <div className="bg-white rounded-lg shadow p-4">
            <h3 className="text-lg font-semibold mb-4">Recent Alerts</h3>
            <div className="space-y-2 max-h-48 overflow-y-auto">
              {alerts.slice(0, 10).map((alert, index) => {
                const formattedAlert = formatAlert(alert);
                return (
                  <div
                    key={index}
                    className="text-xs p-2 rounded border-l-2"
                    style={{
                      backgroundColor: `${formattedAlert.color}10`,
                      borderLeftColor: formattedAlert.color
                    }}
                  >
                    <div style={{ color: formattedAlert.color }}>
                      {formattedAlert.message}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>

          {/* Legend */}
          <div className="bg-white rounded-lg shadow p-4">
            <h3 className="text-lg font-semibold mb-4">Legend</h3>
            <div className="space-y-2 text-sm">
              <div className="flex items-center space-x-2">
                <div className="w-4 h-4 bg-blue-500 rounded"></div>
                <span>Normal Taxi</span>
              </div>
              <div className="flex items-center space-x-2">
                <div className="w-4 h-4 bg-red-500 rounded"></div>
                <span>Speeding Taxi</span>
              </div>
              <div className="flex items-center space-x-2">
                <div className="w-4 h-4 bg-orange-500 rounded"></div>
                <span>Geofence Violation</span>
              </div>
              <div className="flex items-center space-x-2">
                <div className="w-4 h-1 bg-orange-500 rounded"></div>
                <span>Warning Zone (10km)</span>
              </div>
              <div className="flex items-center space-x-2">
                <div className="w-4 h-1 bg-red-500 rounded"></div>
                <span>Drop Zone (15km)</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TaxiFleetDashboard;