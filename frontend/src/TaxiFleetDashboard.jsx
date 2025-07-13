import React, { useState, useEffect } from 'react';
import { MapContainer, TileLayer, Marker, Circle, Popup } from 'react-leaflet';
import { io } from 'socket.io-client';
import L from 'leaflet';

// Fix for default markers in react-leaflet
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl:
    'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
  iconUrl:
    'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
  shadowUrl:
    'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
});

// Custom icons for different taxi states
const normalTaxiIcon = new L.Icon({
  iconUrl:
    'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
  iconRetinaUrl:
    'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
  shadowUrl:
    'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
  shadowSize: [41, 41],
});

const speedingTaxiIcon = new L.Icon({
  iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-red.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
});

const violatingTaxiIcon = new L.Icon({
   iconUrl: 'https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-orange.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
  iconSize: [25, 41],
  iconAnchor: [12, 41],
  popupAnchor: [1, -34],
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
    currentViolations: [],
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

    socket.on('receive_message', data => {
      console.log('Message received:', data);
    });
    socket.on('send_message', data => {
      console.log('Message received:', data);
    });

    socket.on('realTimeData', data => {
      setTaxiLocations(data.locations);
      setSpeeds(data.speeds);
      setAlerts(data.alerts);
      setStatistics(data.statistics);
    });

    // Initial data fetch
    fetchInitialData(BASE_URL);

    return () => socket.disconnect();
  }, []);

  const fetchInitialData = async BASE_URL => {
    try {
      const [locationsRes, speedsRes, alertsRes, statsRes] = await Promise.all([
        fetch(`${BASE_URL}/api/taxi-locations`),
        fetch(`${BASE_URL}/api/speeds`),
        fetch(`${BASE_URL}/api/alerts`),
        fetch(`${BASE_URL}/api/statistics`),
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

  const getTaxiIcon = taxiId => {
    const isSpeeding = statistics.speedingIncidents.some(
      incident => incident.taxiId === taxiId,
    );
    const isViolating = statistics.currentViolations.some(
      violation => violation.taxiId === taxiId,
    );

    if (isSpeeding) return speedingTaxiIcon;
    if (isViolating) return violatingTaxiIcon;
    return normalTaxiIcon;
  };

  const getTaxiSpeed = taxiId => {
    const speedData = speeds.find(s => s.taxiId === taxiId);
    return speedData ? speedData.speed.toFixed(1) : 'N/A';
  };

  const formatAlert = alert => {
    if (alert.includes('SPEEDING')) {
      return { type: 'speeding', message: alert, color: '#ff4444' };
    } else if (alert.includes('GEOFENCE')) {
      return { type: 'geofence', message: alert, color: '#ff8800' };
    }
    return { type: 'other', message: alert, color: '#666' };
  };

  return (
    <div className='min-h-screen bg-gray-100'>
      {/* Header */}
      <div className='bg-white shadow-sm border-b'>
        <div className='px-6 py-4'>
          <div className='flex items-center justify-between'>
            <h1 className='text-2xl font-bold text-gray-900'>
              Taxi Fleet Monitoring
            </h1>
            <div className='flex items-center space-x-2'>
              <div
                className={`w-3 h-3 rounded-full ${
                  connected ? 'bg-green-500' : 'bg-red-500'
                }`}
              ></div>
              <span
                className={`text-sm ${
                  connected ? 'text-green-600' : 'text-red-600'
                }`}
              >
                {connected ? 'Connected' : 'Disconnected'}
              </span>
            </div>
          </div>
        </div>
      </div>

      {/* Main Content - Vertical Layout */}
      <div className='flex flex-col'>
        {/* Map Area */}
        <div className='p-4'>
          <div className='bg-white rounded-lg shadow'>
            <div className='p-4 border-b'>
              <h2 className='text-lg font-semibold text-gray-900'>Live Taxi Locations</h2>
            </div>
            {/* Map container with fixed height */}
            <div className='relative' style={{ height: '60vh' }}>
              <style jsx>{`
                .leaflet-container {
                  height: 100% !important;
                  width: 100% !important;
                  border-radius: 0 0 0.5rem 0.5rem;
                }
                .leaflet-popup-content-wrapper {
                  border-radius: 8px;
                }
                .leaflet-popup-content {
                  margin: 12px 16px;
                  line-height: 1.4;
                }
                .leaflet-control-zoom {
                  border: none !important;
                  border-radius: 8px !important;
                  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1) !important;
                }
                .leaflet-control-zoom a {
                  border: none !important;
                  background-color: white !important;
                  color: #374151 !important;
                  font-size: 18px !important;
                  line-height: 26px !important;
                }
                .leaflet-control-zoom a:hover {
                  background-color: #f3f4f6 !important;
                }
                .leaflet-control-zoom a:first-child {
                  border-radius: 8px 8px 0 0 !important;
                }
                .leaflet-control-zoom a:last-child {
                  border-radius: 0 0 8px 8px !important;
                }
              `}</style>
              <MapContainer
                center={[FORBIDDEN_CITY.lat, FORBIDDEN_CITY.lng]}
                zoom={11}
                style={{ height: '100%', width: '100%' }}
                className='rounded-b-lg'
              >
                <TileLayer
                  url='https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png'
                  attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                />

                {/* Geofence circles */}
                <Circle
                  center={[FORBIDDEN_CITY.lat, FORBIDDEN_CITY.lng]}
                  radius={WARNING_RADIUS}
                  color='#ff8800'
                  fillColor='#ff8800'
                  fillOpacity={0.1}
                  weight={2}
                  dashArray="5, 5"
                />
                <Circle
                  center={[FORBIDDEN_CITY.lat, FORBIDDEN_CITY.lng]}
                  radius={DROP_RADIUS}
                  color='#ff4444'
                  fillColor='#ff4444'
                  fillOpacity={0.05}
                  weight={2}
                  dashArray="10, 5"
                />

                {/* Taxi markers */}
                {taxiLocations.map(taxi => (
                  <Marker
                    key={taxi.taxiId}
                    position={[taxi.latitude, taxi.longitude]}
                    icon={getTaxiIcon(taxi.taxiId)}
                  >
                    <Popup>
                      <div className='text-sm min-w-48'>
                        <div className='font-semibold text-gray-900 mb-2'>
                          Taxi {taxi.taxiId}
                        </div>
                        <div className='space-y-1'>
                          <div className='flex justify-between'>
                            <span className='text-gray-600'>Speed:</span>
                            <span className='font-medium'>{getTaxiSpeed(taxi.taxiId)} km/h</span>
                          </div>
                          <div className='flex justify-between'>
                            <span className='text-gray-600'>Latitude:</span>
                            <span className='font-mono text-xs'>{taxi.latitude.toFixed(6)}</span>
                          </div>
                          <div className='flex justify-between'>
                            <span className='text-gray-600'>Longitude:</span>
                            <span className='font-mono text-xs'>{taxi.longitude.toFixed(6)}</span>
                          </div>
                          <div className='flex justify-between'>
                            <span className='text-gray-600'>Updated:</span>
                            <span className='text-xs'>{new Date(taxi.timestamp).toLocaleTimeString()}</span>
                          </div>
                        </div>
                      </div>
                    </Popup>
                  </Marker>
                ))}
              </MapContainer>
            </div>
          </div>
        </div>

        {/* Sidebar - Now below the map */}
        <div className='p-4 pt-0'>
          <div className='grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 xl:grid-cols-5 gap-4'>
            
            {/* Statistics Cards */}
            <div className='bg-white rounded-lg shadow p-4'>
              <h3 className='text-lg font-semibold mb-4 text-gray-900'>Fleet Statistics</h3>
              <div className='space-y-3'>
                <div className='flex justify-between items-center'>
                  <span className='text-gray-600'>Currently Driving</span>
                  <span className='font-semibold text-blue-600 bg-blue-50 px-2 py-1 rounded'>
                    {taxiLocations.length} Taxis
                  </span>
                </div>
                <div className='flex justify-between items-center'>
                  <span className='text-gray-600'>Total Distance</span>
                  <span className='font-semibold text-gray-600'>
                    {statistics.totalDistance.toFixed(2)} km
                  </span>
                </div>
                <div className='flex justify-between items-center'>
                  <span className='text-red-600'>Speeding Taxis</span>
                  <span className='font-semibold text-red-600 bg-red-50 px-2 py-1 rounded'>
                    {statistics.speedingIncidents.length}
                  </span>
                </div>
                <div className='flex justify-between items-center'>
                  <span className='text-orange-600'>Area Violations</span>
                  <span className='font-semibold text-orange-600 bg-orange-50 px-2 py-1 rounded'>
                    {statistics.currentViolations.length}
                  </span>
                </div>
              </div>
            </div>

            {/* Current Incidents */}
            <div className='bg-white rounded-lg shadow p-4'>
              <h3 className='text-lg font-semibold mb-4 text-gray-900'>Current Incidents</h3>
              <div className='space-y-2 overflow-y-auto' style={{ height: '300px' }}>
                {statistics.speedingIncidents.map((incident, index) => (
                  <div
                    key={index}
                    className='text-sm p-3 bg-red-50 rounded-lg border-l-4 border-red-400'
                  >
                    <div className='font-medium text-red-700'>
                      Taxi {incident.taxiId}
                    </div>
                    <div className='text-red-600'>Speeding: {incident.speed}</div>
                  </div>
                ))}
                {statistics.currentViolations.map((violation, index) => (
                  <div
                    key={index}
                    className='text-sm p-3 bg-orange-50 rounded-lg border-l-4 border-orange-400'
                  >
                    <div className='font-medium text-orange-700'>
                      Taxi {violation.taxiId}
                    </div>
                    <div className='text-orange-600'>Area Violation</div>
                  </div>
                ))}
              </div>
            </div>

            {/* Recent Alerts */}
            <div className='bg-white rounded-lg shadow p-4'>
              <h3 className='text-lg font-semibold mb-4 text-gray-900'>Recent Alerts</h3>
              <div className='space-y-2 overflow-y-auto' style={{ height: '300px' }}>
                {alerts.slice(0, 100).map((alert, index) => {
                  const formattedAlert = formatAlert(alert);
                  return (
                    <div
                      key={index}
                      className='text-xs p-3 rounded-lg border-l-4'
                      style={{
                        backgroundColor: `${formattedAlert.color}10`,
                        borderLeftColor: formattedAlert.color,
                      }}
                    >
                      <div style={{ color: formattedAlert.color }} className='font-medium'>
                        {formattedAlert.message}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Legend */}
            <div className='bg-white rounded-lg shadow p-4'>
              <h3 className='text-lg font-semibold mb-4 text-gray-900'>Legend</h3>
              <div className='space-y-3 text-sm'>
                <div className='flex items-center space-x-3'>
                  <div className='w-5 h-5 bg-blue-500 rounded-full border-2 border-blue-600'></div>
                  <span className='text-gray-900'>Normal Taxi</span>
                </div>
                <div className='flex items-center space-x-3'>
                  <div className='w-5 h-5 bg-red-500 rounded-full border-2 border-red-600'></div>
                  <span className='text-gray-900'>Speeding Taxi</span>
                </div>
                <div className='flex items-center space-x-3'>
                  <div className='w-5 h-5 bg-orange-500 rounded-full border-2 border-orange-600'></div>
                  <span className='text-gray-900'>Geofence Violation</span>
                </div>
                <div className='flex items-center space-x-3'>
                  <div className='w-8 h-2 bg-orange-500 rounded' style={{ background: 'repeating-linear-gradient(90deg, #ff8800, #ff8800 5px, transparent 5px, transparent 10px)' }}></div>
                  <span className='text-gray-900'>Warning Zone (10km)</span>
                </div>
                <div className='flex items-center space-x-3'>
                  <div className='w-8 h-2 bg-red-500 rounded' style={{ background: 'repeating-linear-gradient(90deg, #ff4444, #ff4444 10px, transparent 10px, transparent 15px)' }}></div>
                  <span className='text-gray-900'>Drop Zone (15km)</span>
                </div>
              </div>
            </div>

            {/* Empty column for better spacing on larger screens */}
            <div className='hidden xl:block'></div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TaxiFleetDashboard;