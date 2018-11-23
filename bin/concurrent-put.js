const Request = require('request');

Request.put({url: 'http://127.0.0.1:3000/store/concurrent', json: {server: 1}});
Request.put({url: 'http://127.0.0.1:3001/store/concurrent', json: {server: 2}});
Request.put({url: 'http://127.0.0.1:3002/store/concurrent', json: {server: 3}});

setTimeout(readAnswer, 500);

function readAnswer() {
    Request.get({url: 'http://127.0.0.1:3000/store/concurrent', json: true}, showResponse);
    Request.get({url: 'http://127.0.0.1:3001/store/concurrent', json: true}, showResponse);
    Request.get({url: 'http://127.0.0.1:3002/store/concurrent', json: true}, showResponse);
}

function showResponse(err, resp, body) {
    console.log(this.uri.port + ": " + body.server);
}
