module.exports = function (RED) {
    "use strict";

    function BigQueryNode(n) {
        RED.nodes.createNode(this, n);
        if (this.credentials
                && this.credentials.private_key_id
                && this.credentials.private_key
                && this.credentials.client_email
                && this.credentials.client_id
                && this.credentials.type
                && this.credentials.projectId) {

            this.gcloud = require('gcloud');
            this.bigquery = this.gcloud.bigquery({
                projectId: this.credentials.projectId,
                credentials: {
                    "private_key_id": this.credentials.private_key_id,
                    "private_key": this.credentials.private_key.replace(/\\n/g, "\n"),
                    "client_email": this.credentials.client_email,
                    "client_id": this.credentials.client_id,
                    "type": this.credentials.type
                }
            });
        }
    }

    RED.nodes.registerType("bigquery-config", BigQueryNode, {
        credentials: {
            private_key_id: { type: "text" },
            private_key: { type: "password" },
            client_email: { type: "text" },
            client_id: { type: "text" },
            type: { type: "text" },
            projectId: { type: "text" }
        }
    });

    function BigQueryQueryNode(n) {
        RED.nodes.createNode(this, n);
        this.bigquery_node = RED.nodes.getNode(n.bigquery);
        this.query = n.query;
        var bigquery = this.bigquery_node.bigquery || null,
            node = this;
        if (!bigquery) {
            node.warn("gcp.warn.missing-credentials");
            return;
        }
        node.on("input", function (msg) {
            node.status({ fill: "blue", shape: "dot", text: "gcp.status.querying" });
            bigquery.query(node.query, function (err, rows) {
                if (err) {
                    node.error("gcp.error.query-failed", msg);
                }
                msg.payload = rows;
                node.status({});
                node.send(msg);
            });
        });
    }
    RED.nodes.registerType("bigquery query", BigQueryQueryNode);

    function BigQueryInsertNode(n) {
        RED.nodes.createNode(this, n);
        this.bigquery_node = RED.nodes.getNode(n.bigquery);
        this.dataset = n.dataset;
        this.table = n.table;
        var node = this,
            bigquery = this.bigquery_node.bigquery;
        if (!bigquery) {
            node.warn("gcp.warn.missing-credentials");
            return;
        }
        if (!this.dataset && !this.table) {
            node.warn("gcp.warn.no-dataset-table-specified");
            return;
        }

        node.on("input", function (msg) {
            var dataset = bigquery.dataset(node.dataset),
                table = dataset.table(node.table),
                insert_data = JSON.parse(msg.payload);

            table.insert(insert_data, function (err, insertErrors) {
                if (err) {
                    node.error("gcp.error.general-error", msg);
                    return;
                }
                if (insertErrors.length > 0) {
                    node.error("gcp.error.insert-error", msg);
                    return;
                }
            });
        });
    }
    RED.nodes.registerType("bigquery insert", BigQueryInsertNode);
};
