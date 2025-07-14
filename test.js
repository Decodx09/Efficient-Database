const http = require('http');
const { performance } = require('perf_hooks');

class SimpleLoadTest {
  constructor() {
    this.results = {
      requests: 0,
      errors: 0,
      totalTime: 0,
      responseTypes: {},
      avgResponseTime: 0
    };
  }

  async runTest(url, concurrency = 10, duration = 30) {
    console.log(`🚀 Testing ${url} with ${concurrency} concurrent users for ${duration}s`);
    
    const startTime = performance.now();
    const endTime = startTime + (duration * 1000);
    
    const workers = [];
    for (let i = 0; i < concurrency; i++) {
      workers.push(this.worker(url, endTime));
    }
    
    await Promise.all(workers);
    
    const totalTestTime = (performance.now() - startTime) / 1000;
    this.printResults(totalTestTime);
  }

  async worker(url, endTime) {
    while (performance.now() < endTime) {
      const startTime = performance.now();
      
      try {
        const response = await this.makeRequest(url);
        const responseTime = performance.now() - startTime;
        
        this.results.requests++;
        this.results.totalTime += responseTime;
        this.results.responseTypes[response.statusCode] = 
          (this.results.responseTypes[response.statusCode] || 0) + 1;
          
      } catch (error) {
        this.results.errors++;
      }
    }
  }

  makeRequest(url) {
    return new Promise((resolve, reject) => {
      const req = http.request(url, (res) => {
        let data = '';
        res.on('data', chunk => data += chunk);
        res.on('end', () => resolve(res));
      });
      req.on('error', reject);
      req.setTimeout(5000, () => reject(new Error('Timeout')));
      req.end();
    });
  }

  printResults(testDuration) {
    console.log('\n📊 PERFORMANCE RESULTS:');
    console.log('='.repeat(50));
    console.log(`Total Requests: ${this.results.requests}`);
    console.log(`Errors: ${this.results.errors}`);
    console.log(`Success Rate: ${((this.results.requests - this.results.errors) / this.results.requests * 100).toFixed(2)}%`);
    console.log(`Requests/Second: ${(this.results.requests / testDuration).toFixed(2)}`);
    console.log(`Average Response Time: ${(this.results.totalTime / this.results.requests).toFixed(2)}ms`);
    console.log(`Test Duration: ${testDuration.toFixed(2)}s`);
    console.log('\n📈 Response Codes:', this.results.responseTypes);
    
    // Simple performance rating
    const rps = this.results.requests / testDuration;
    const avgTime = this.results.totalTime / this.results.requests;
    
    console.log('\n🎯 PERFORMANCE RATING:');
    if (rps > 100 && avgTime < 100) console.log('🟢 EXCELLENT - Ready for production!');
    else if (rps > 50 && avgTime < 200) console.log('🟡 GOOD - Needs minor optimization');
    else if (rps > 20 && avgTime < 500) console.log('🟠 AVERAGE - Requires optimization');
    else console.log('🔴 POOR - Needs major improvements');
  }
}

// Run the test
const tester = new SimpleLoadTest();
const url = process.argv[2] || 'http://localhost:3000/api/users';
const concurrency = parseInt(process.argv[3]) || 10;
const duration = parseInt(process.argv[4]) || 30;

tester.runTest(url, concurrency, duration);