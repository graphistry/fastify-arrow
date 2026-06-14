const { PassThrough } = require('stream');
const { as: asAsyncIterable } = require('ix/asynciterable');
const { RecordBatchReader } = require('apache-arrow');

// fastify@5 + apache-arrow@21 compat (graphistry fork, `fastify5` branch).
// Changes from the v3 `graphistry` branch:
//  1. addContentTypeParser content-type must be a full MIME under fastify@5
//     ('octet-stream' -> 'application/octet-stream'; fastify@5 rejects the bare token via
//     ContentType.isValid === false -> FST_ERR_CTP_INVALID_TYPE).
//  2. fastify-plugin metadata constraint bumped to '5.x'.
//  3. Multipart support DROPPED. Every fastify-arrow consumer in the graphistry stack
//     (gpu-worker /post|/encode|/filter|..., /open|/read|/play|/stop|/tick; sessions; gpu-router)
//     sends/receives application/octet-stream only; nothing posts multipart/form-data to a
//     fastify-arrow endpoint (the one multipart sender, shapeGraphViaForm, targets
//     forge-etl-python, not this plugin). Dropping @fastify/multipart also avoids the
//     v6->v9 callback-API rewrite (`request.multipart()` -> parts()/file()) that fastify@5
//     would otherwise force.
module.exports = require('fastify-plugin')(fastifyArrowPlugin, {
  fastify: '5.x', name: 'fastify-arrow'
});

function fastifyArrowPlugin(fastify, opts, next) {

  // Stub octet-stream parser so fastify doesn't reject Arrow IPC payloads.
  fastify.addContentTypeParser('application/octet-stream', async (request, payload) => payload);

  fastify.decorateReply('stream', replyAsStream);
  fastify.decorateRequest('recordBatches', readRecordBatches);

  next();
}

function replyAsStream(xs = { objectMode: false }) {
  const stream = new PassThrough(xs);
  this.send(stream);
  return stream;
}

/**
 * @returns AsyncIterable<RecordBatchReader>
 */
function readRecordBatches() {
  const source = this.raw.pipe(new PassThrough());
  return asAsyncIterable(RecordBatchReader.readAll(source));
}
