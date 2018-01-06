node-red-contrib-bigquery
=========================
A <a href="http://nodered.org" target="_new">Node-RED</a> node to query and insert data in Google BigQuery.

Install
-------

Run the following command in your Node-RED user directory - typically `~/.node-red`

    npm install node-red-contrib-bigquery

Authentication
--------------
Although Google Cloud Platform provides several ways to authenticate requests, this node only supports private-key authentication of service account. New service account can be created by following <a href="https://developers.google.com/identity/protocols/OAuth2ServiceAccount#creatinganaccount">link</a>.

Usage
-----

### BigQuery Query Node

BigQuery query node. Query data in BigQuery. The query statement(currently BigQuery only supports SELECT statement) must be specified in the node property. If the query succeeded, the results are returned in `msg.payload`. Currently, this node supports only single statement.

### BigQuery Insert Node

BigQuery insert node. Insert mutiple data into BigQuery table. The dataset name and the table name must be specified in **the node property**. The insert data is taken from `msg.payload` which supports both single data or multiple data.<
