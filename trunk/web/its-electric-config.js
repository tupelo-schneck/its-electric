itsElectric.configure({
   datasourceURL: 'http://localhost:8081',
   hasVoltage: false,
   hasKVA: false,
   initialZoom: 4*60*60,
   realTime: true, // set false to prevent automatic update when viewing latest time
   realTimeUpdateInterval: 60000,
   partialRange: false, // when true, graph only includes points near zoomed-in region
   noFlashEvents: false
});