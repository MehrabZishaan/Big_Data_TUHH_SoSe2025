
function Map({ taxiLocations }) {
    // Forbidden City coordinates and geofence circles
    const FORBIDDEN_CITY = { lat: 39.916, lng: 116.397 };
    const WARNING_RADIUS = 10000; // 10km in meters
    const DROP_RADIUS = 15000; // 15km in meters


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
  return (
    <MapContainer
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
  );
}
export default Map;