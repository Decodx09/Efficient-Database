const rps = 75; // Your test result
const avgResponseTime = 120; // milliseconds
const peakHours = 8; // hours per day
const growthFactor = 1.5; // 50% growth buffer

const dailyRequests = rps * 60 * 60 * peakHours;
const maxUsers = Math.floor(rps * avgResponseTime / 1000);
const scaledCapacity = Math.floor(maxUsers * growthFactor);

console.log(`📈 CAPACITY ESTIMATES:`);
console.log(`Current RPS: ${rps}`);
console.log(`Daily Requests: ${dailyRequests.toLocaleString()}`);
console.log(`Max Concurrent Users: ${maxUsers}`);
console.log(`Recommended Capacity: ${scaledCapacity} users`);
console.log(`Monthly Requests: ${(dailyRequests * 30).toLocaleString()}`);

