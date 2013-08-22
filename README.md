nodeora
=======

Node.js oracle addon for async execution.


sample
======

var nodeora = require('nodeora');
var connection = new nodeora.Connection();

connection.open("UserName", "Password", "TNS");

connection.query("SELECT NAME FROM SOMETABLE", function (rows) { console.log(rows);});    	
connection.close();