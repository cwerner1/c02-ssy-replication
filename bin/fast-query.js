const Request = require('request');

let value = Date.now();
startCall();

function startCall() {
    value = Date.now();
    console.log('Putting ' + value);
    Request.put({url: 'http://127.0.0.1:3000/store/key-fast', json: {wert: value}}, nextCall);
}

function nextCall(err, resp, body) {
    console.log('Getting ' + value);
    Request.get({url: 'http://127.0.0.1:3002/store/key-fast', json: true}, resultCall);
}

function resultCall(err, resp, body) {
    if (body === null || body.wert !== value) {
        console.log("!! Mismatch !!");
        console.log(body);
        process.exit();
    }
    startCall();
}
